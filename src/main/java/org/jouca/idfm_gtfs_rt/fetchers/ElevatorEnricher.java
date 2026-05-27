package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

import com.fasterxml.jackson.databind.*;
import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.*;

/**
 * Enriches an already-enriched GTFS ZIP with elevator pathway data sourced from
 * the IDFM open-data API ({@code etat-des-ascenseurs}).
 *
 * <p>For each elevator entry the enricher:
 * <ol>
 *   <li>Resolves the parent station via {@code zdcid} → {@code IDFM:{zdcid}} in
 *       {@code stops.txt}.</li>
 *   <li>Finds the nearest existing entrance ({@code location_type=2}) in that
 *       station, or creates a virtual elevator stop ({@code location_type=2}) when
 *       none exists.</li>
 *   <li>Finds the nearest platform ({@code location_type=0}) in the same station.</li>
 *   <li>Emits a {@code pathways.txt} row with {@code pathway_mode=5} (elevator),
 *       {@code is_bidirectional=1}, and a traversal time estimated at 0.5 m/s
 *       (minimum 30 s).</li>
 * </ol>
 *
 * <p>Any pre-existing {@code pathways.txt} in the input ZIP is preserved; new
 * elevator pathways are appended (duplicate {@code pathway_id} values are
 * skipped).
 */
public class ElevatorEnricher {

    private static final Logger logger = LoggerFactory.getLogger(ElevatorEnricher.class);

    private static final Dotenv dotenv = Dotenv.configure().directory("/app").ignoreIfMissing().load();

    static final String ELEVATORS_URL =
        "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/etat-des-ascenseurs/exports/json";

    // -------------------------------------------------------------------------
    // Internal value type
    // -------------------------------------------------------------------------

    record StopRecord(String stopId, double lat, double lon, int locationType, String parentStation) {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Reads {@code inputZipPath} (an already-enriched GTFS ZIP), adds elevator
     * stops and pathways, and writes the result to {@code outputZipPath}.
     *
     * @param inputZipPath  path to the enriched GTFS ZIP to read
     * @param outputZipPath path for the output ZIP (may equal inputZipPath — a
     *                      temporary file is used internally in that case)
     * @throws IOException on network or file errors
     */
    public static void addElevatorPathways(String inputZipPath, String outputZipPath) throws IOException {
        logger.info("Starting elevator pathway enrichment…");

        List<JsonNode> elevators = fetchElevators();
        if (elevators.isEmpty()) {
            logger.warn("No elevator data fetched; skipping pathways.txt generation");
            if (!inputZipPath.equals(outputZipPath)) {
                Files.copy(Paths.get(inputZipPath), Paths.get(outputZipPath),
                    StandardCopyOption.REPLACE_EXISTING);
            }
            return;
        }
        logger.info("Fetched {} elevators from IDFM API", elevators.size());

        Map<String, List<StopRecord>> stopsByStation = new LinkedHashMap<>();
        collectStops(inputZipPath, stopsByStation);
        logger.info("Collected stops for {} stations", stopsByStation.size());

        List<StopRecord> newElevatorStops = new ArrayList<>();
        List<String[]> newPathwayRows = new ArrayList<>();
        buildPathways(elevators, stopsByStation, newElevatorStops, newPathwayRows);
        logger.info("Generated {} elevator pathways, {} new elevator stops",
            newPathwayRows.size(), newElevatorStops.size());

        rewriteZipWithPathways(inputZipPath, outputZipPath, newElevatorStops, newPathwayRows);
        logger.info("Elevator-enriched GTFS written to {}", outputZipPath);
    }

    // -------------------------------------------------------------------------
    // Data fetching
    // -------------------------------------------------------------------------

    public static List<JsonNode> fetchElevators() throws IOException {
        URL url = URI.create(ELEVATORS_URL).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Accept-Encoding", "gzip");
        conn.setConnectTimeout(30_000);
        conn.setReadTimeout(120_000);

        String odsToken = dotenv.get("ODS_TOKEN", null);
        if (odsToken != null && !odsToken.isBlank()) {
            conn.setRequestProperty("Authorization", "Apikey " + odsToken);
        }

        List<JsonNode> result = new ArrayList<>();
        try {
            InputStream raw = conn.getInputStream();
            String encoding = conn.getContentEncoding();
            InputStream is = (encoding != null && encoding.equalsIgnoreCase("gzip"))
                ? new GZIPInputStream(raw)
                : raw;
            try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(isr);
                if (root.isArray()) {
                    for (JsonNode el : root) {
                        String liftId = el.path("liftid").asText(null);
                        String zdcId  = el.path("zdcid").asText(null);
                        if (liftId != null && !liftId.isBlank()
                                && zdcId  != null && !zdcId.isBlank()) {
                            result.add(el);
                        }
                    }
                }
            }
        } finally {
            conn.disconnect();
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Stop collection (read-only pass)
    // -------------------------------------------------------------------------

    private static void collectStops(String zipPath,
            Map<String, List<StopRecord>> stopsByStation) throws IOException {
        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(Paths.get(zipPath)))) {
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if ("stops.txt".equals(entry.getName())) {
                    parseStops(zin.readAllBytes(), stopsByStation);
                }
                zin.closeEntry();
            }
        }
    }

    private static void parseStops(byte[] bytes,
            Map<String, List<StopRecord>> stopsByStation) throws IOException {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8));

        String headerLine = reader.readLine();
        if (headerLine == null) return;

        String[] headers = GTFSEnricher.parseCsvLine(headerLine);
        int stopIdIdx = -1, latIdx = -1, lonIdx = -1, typeIdx = -1, parentIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            switch (headers[i].trim()) {
                case "stop_id"        -> stopIdIdx = i;
                case "stop_lat"       -> latIdx     = i;
                case "stop_lon"       -> lonIdx     = i;
                case "location_type"  -> typeIdx    = i;
                case "parent_station" -> parentIdx  = i;
            }
        }

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;
            String[] fields = GTFSEnricher.parseCsvLine(line);
            String stopId       = field(fields, stopIdIdx);
            String latStr       = field(fields, latIdx);
            String lonStr       = field(fields, lonIdx);
            String parentStation = field(fields, parentIdx);
            int locationType = 0;
            try { locationType = Integer.parseInt(field(fields, typeIdx)); }
            catch (NumberFormatException ignored) {}

            if (stopId.isBlank() || latStr.isBlank() || lonStr.isBlank()) continue;
            try {
                double lat = Double.parseDouble(latStr);
                double lon = Double.parseDouble(lonStr);
                StopRecord rec = new StopRecord(stopId, lat, lon, locationType, parentStation);
                String key = parentStation.isBlank() ? stopId : parentStation;
                stopsByStation.computeIfAbsent(key, k -> new ArrayList<>()).add(rec);
            } catch (NumberFormatException ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Pathway building
    // -------------------------------------------------------------------------

    private static void buildPathways(
            List<JsonNode> elevators,
            Map<String, List<StopRecord>> stopsByStation,
            List<StopRecord> newElevatorStops,
            List<String[]> pathwayRows) {

        Set<String> usedPathwayIds  = new HashSet<>();
        Set<String> usedPathwayKeys = new HashSet<>();

        for (JsonNode elevator : elevators) {
            String rawLiftId = elevator.path("liftid").asText("").replaceAll("[^a-zA-Z0-9_\\-]", "_");
            String zdcId     = elevator.path("zdcid").asText("");
            if (rawLiftId.isBlank() || zdcId.isBlank()) continue;

            String parentStation = "IDFM:" + zdcId;
            List<StopRecord> stopsInStation = stopsByStation.get(parentStation);
            if (stopsInStation == null || stopsInStation.isEmpty()) continue;

            JsonNode centroid = elevator.path("centroidzdc");
            double elLat = centroid.path("lat").asDouble(Double.NaN);
            double elLon = centroid.path("lon").asDouble(Double.NaN);
            if (Double.isNaN(elLat) || Double.isNaN(elLon)) continue;

            StopRecord nearestEntrance = nearest(stopsInStation, elLat, elLon, 2);
            StopRecord nearestPlatform = nearest(stopsInStation, elLat, elLon, 0);
            if (nearestPlatform == null) continue;

            // Determine from-stop: existing entrance or newly-created elevator stop
            String fromStopId;
            double fromLat, fromLon;
            if (nearestEntrance != null) {
                fromStopId = nearestEntrance.stopId();
                fromLat    = nearestEntrance.lat();
                fromLon    = nearestEntrance.lon();
            } else {
                String elevStopId = "IDFM:elevator:" + rawLiftId;
                boolean alreadyCreated = newElevatorStops.stream()
                    .anyMatch(s -> s.stopId().equals(elevStopId));
                if (!alreadyCreated) {
                    StopRecord elevStop = new StopRecord(elevStopId, elLat, elLon, 2, parentStation);
                    newElevatorStops.add(elevStop);
                    stopsInStation.add(elevStop);
                }
                fromStopId = elevStopId;
                fromLat    = elLat;
                fromLon    = elLon;
            }

            String pathwayKey = fromStopId + "|" + nearestPlatform.stopId();
            if (usedPathwayKeys.contains(pathwayKey)) continue;
            usedPathwayKeys.add(pathwayKey);

            // Unique pathway_id (multiple elevators may share same from/to pair)
            String pathwayId = "elevator_" + rawLiftId;
            if (usedPathwayIds.contains(pathwayId)) pathwayId = pathwayId + "_" + pathwayRows.size();
            usedPathwayIds.add(pathwayId);

            double length       = haversine(fromLat, fromLon, nearestPlatform.lat(), nearestPlatform.lon());
            int traversalTime   = Math.max(30, (int) Math.round(length / 0.5));

            pathwayRows.add(new String[]{
                pathwayId,
                fromStopId,
                nearestPlatform.stopId(),
                "5",
                "1",
                String.format(java.util.Locale.US, "%.1f", length),
                String.valueOf(traversalTime)
            });
        }
    }

    private static StopRecord nearest(List<StopRecord> stops, double lat, double lon, int locationType) {
        StopRecord best = null;
        double bestDist = Double.MAX_VALUE;
        for (StopRecord s : stops) {
            if (s.locationType() != locationType) continue;
            double d = haversine(lat, lon, s.lat(), s.lon());
            if (d < bestDist) { bestDist = d; best = s; }
        }
        return best;
    }

    static double haversine(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6_371_000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                 + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                 * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // -------------------------------------------------------------------------
    // ZIP rewriting
    // -------------------------------------------------------------------------

    private static void rewriteZipWithPathways(
            String inputZipPath, String outputZipPath,
            List<StopRecord> newElevatorStops,
            List<String[]> newPathwayRows) throws IOException {

        Path inputPath  = Paths.get(inputZipPath);
        Path outputPath = Paths.get(outputZipPath);

        // Collect existing pathways.txt rows (to merge, not overwrite)
        List<String[]> existingPathwayRows = new ArrayList<>();

        // First pass: find any existing pathways.txt
        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(inputPath))) {
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if ("pathways.txt".equals(entry.getName())) {
                    existingPathwayRows = parseExistingPathways(zin.readAllBytes());
                }
                zin.closeEntry();
            }
        }

        // Build the merged pathway list (existing rows first, then new elevator rows)
        Set<String> existingPathwayIds = new HashSet<>();
        for (String[] row : existingPathwayRows) {
            if (row.length > 0) existingPathwayIds.add(row[0]);
        }
        List<String[]> mergedPathways = new ArrayList<>(existingPathwayRows);
        for (String[] row : newPathwayRows) {
            if (!existingPathwayIds.contains(row[0])) {
                mergedPathways.add(row);
            }
        }

        // Second pass: rewrite ZIP
        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(inputPath));
             ZipOutputStream zout = new ZipOutputStream(Files.newOutputStream(outputPath))) {

            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                String name = entry.getName();

                if ("pathways.txt".equals(name)) {
                    // Handled separately after the loop
                    zin.closeEntry();
                    continue;
                }

                zout.putNextEntry(new ZipEntry(name));
                if ("stops.txt".equals(name) && !newElevatorStops.isEmpty()) {
                    byte[] original = zin.readAllBytes();
                    zout.write(appendElevatorStops(original, newElevatorStops));
                } else {
                    zin.transferTo(zout);
                }
                zout.closeEntry();
                zin.closeEntry();
            }

            // Write (merged) pathways.txt
            zout.putNextEntry(new ZipEntry("pathways.txt"));
            zout.write(buildPathwaysTxt(mergedPathways));
            zout.closeEntry();
        }
    }

    /**
     * Parses an existing {@code pathways.txt} byte array into a list of field
     * arrays (header row is skipped; each element is one pathway row).
     */
    private static List<String[]> parseExistingPathways(byte[] bytes) throws IOException {
        List<String[]> rows = new ArrayList<>();
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8));
        reader.readLine(); // skip header
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.trim().isEmpty()) rows.add(GTFSEnricher.parseCsvLine(line));
        }
        return rows;
    }

    /**
     * Appends virtual elevator stop rows to an existing {@code stops.txt} byte
     * array, preserving the original header and column order.
     */
    private static byte[] appendElevatorStops(byte[] original, List<StopRecord> elevatorStops)
            throws IOException {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream(original), StandardCharsets.UTF_8));
        String headerLine = reader.readLine();
        if (headerLine == null) return original;

        String[] headers = GTFSEnricher.parseCsvLine(headerLine);
        String content = new String(original, StandardCharsets.UTF_8).stripTrailing();
        StringBuilder sb = new StringBuilder(content).append("\n");

        for (StopRecord stop : elevatorStops) {
            String[] fields = new String[headers.length];
            Arrays.fill(fields, "");
            for (int i = 0; i < headers.length; i++) {
                switch (headers[i].trim()) {
                    case "stop_id"            -> fields[i] = stop.stopId();
                    case "stop_name"          -> fields[i] = "Ascenseur";
                    case "stop_lat"           -> fields[i] = String.format(java.util.Locale.US, "%.8f", stop.lat());
                    case "stop_lon"           -> fields[i] = String.format(java.util.Locale.US, "%.8f", stop.lon());
                    case "location_type"      -> fields[i] = String.valueOf(stop.locationType());
                    case "parent_station"     -> fields[i] = stop.parentStation();
                    case "stop_timezone"      -> fields[i] = "Europe/Paris";
                    case "wheelchair_boarding"-> fields[i] = "1";
                    default -> {}
                }
            }
            sb.append(GTFSEnricher.joinCsvLine(fields)).append("\n");
        }

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] buildPathwaysTxt(List<String[]> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("pathway_id,from_stop_id,to_stop_id,pathway_mode,is_bidirectional,length,traversal_time\n");
        for (String[] row : rows) {
            sb.append(GTFSEnricher.joinCsvLine(row)).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String field(String[] fields, int idx) {
        return idx >= 0 && idx < fields.length ? fields[idx].trim() : "";
    }
}
