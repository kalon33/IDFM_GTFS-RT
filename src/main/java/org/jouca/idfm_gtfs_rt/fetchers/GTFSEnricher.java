package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import com.fasterxml.jackson.databind.*;
import org.slf4j.*;

/**
 * Enriches a GTFS ZIP file's {@code stops.txt} with data sourced from IDFM
 * open-data APIs.
 *
 * <p>Two operations are performed:
 * <ol>
 *   <li><b>Enrich existing stops</b>: fills the {@code platform_code} column
 *       for stops whose {@code stop_id} is {@code IDFM:{digits}} using the
 *       {@code publiccode} field from {@code arrets-transporteur.json}.</li>
 *   <li><b>Create missing stops</b>: for each entry in
 *       {@code arrets-transporteur.json} whose {@code arrid} has no
 *       corresponding GTFS stop, a new row is appended using coordinates and
 *       name from the JSON and the {@code parent_station} from
 *       {@code relations.csv}.</li>
 * </ol>
 *
 * <p>Data sources:
 * <ul>
 *   <li>arrets-transporteur JSON — arrid, publiccode, artname, artgeopoint,
 *       artfarezone</li>
 *   <li>relations CSV — arrid → zdcid (parent multimodal stop ID)</li>
 * </ul>
 */
public class GTFSEnricher {

    private static final Logger logger = LoggerFactory.getLogger(GTFSEnricher.class);

    static final String ARRETS_TRANSPORTEUR_URL =
        "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/arrets-transporteur/exports/json";

    static final String RELATIONS_URL =
        "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/relations/exports/csv";

    // Matches only pure-numeric stop IDs: "IDFM:471134"
    private static final Pattern PURE_NUMERIC_STOP_ID = Pattern.compile("^IDFM:(\\d+)$");

    // Detects station-name strings (4+ consecutive letters)
    private static final Pattern STATION_NAME_LETTERS = Pattern.compile("[a-zA-ZÀ-ÿ]{4,}");

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Produces an enriched GTFS ZIP at {@code outputZipPath} from the original
     * GTFS ZIP at {@code inputZipPath}.
     *
     * <p>The enrichment consists of:
     * <ul>
     *   <li>Filling {@code platform_code} in {@code stops.txt} for existing
     *       quay-level stops (pure numeric {@code stop_id}).</li>
     *   <li>Appending new rows for quays present in arrets-transporteur but
     *       absent from the GTFS, provided their parent station can be resolved
     *       from relations.csv.</li>
     * </ul>
     *
     * @param inputZipPath  path to the original GTFS ZIP (e.g. IDFM-gtfs.zip)
     * @param outputZipPath path for the enriched output ZIP
     * @throws IOException on network or file errors
     */
    public static void enrichGTFS(String inputZipPath, String outputZipPath) throws IOException {
        logger.info("Starting GTFS enrichment (platform_code + missing stops)…");

        Map<String, JsonNode> arretData = fetchArretData();
        logger.info("Fetched {} arret entries from arrets-transporteur", arretData.size());

        Map<String, String> arridToZdcid = fetchRelations();
        logger.info("Fetched {} arrid→zdcid relations", arridToZdcid.size());

        rewriteZip(inputZipPath, outputZipPath, arretData, arridToZdcid);
        logger.info("Enriched GTFS written to {}", outputZipPath);
    }

    // -------------------------------------------------------------------------
    // Data fetching
    // -------------------------------------------------------------------------

    /**
     * Downloads arrets-transporteur JSON and returns a map of
     * {@code arrid → JsonNode} (full entry).
     *
     * <p>Entries with a null or blank {@code arrid} are skipped.
     *
     * @return arrid-keyed map of full JSON entries
     * @throws IOException on network errors
     */
    static Map<String, JsonNode> fetchArretData() throws IOException {
        URL url = URI.create(ARRETS_TRANSPORTEUR_URL).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Accept-Encoding", "gzip");
        conn.setConnectTimeout(30_000);
        conn.setReadTimeout(120_000);

        Map<String, JsonNode> result = new LinkedHashMap<>();
        try {
            InputStream raw = conn.getInputStream();
            String encoding = conn.getContentEncoding();
            InputStream is = (encoding != null && encoding.equalsIgnoreCase("gzip"))
                    ? new java.util.zip.GZIPInputStream(raw)
                    : raw;
            try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8)) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(isr);
                if (root.isArray()) {
                    for (JsonNode entry : root) {
                        String arrid = entry.path("arrid").asText(null);
                        if (arrid != null && !arrid.isBlank()) {
                            result.put(arrid, entry);
                        }
                    }
                }
            }
        } finally {
            conn.disconnect();
        }
        return result;
    }

    /**
     * Downloads relations.csv and returns a map of {@code arrid → zdcid}.
     *
     * <p>The CSV header is read dynamically so column order does not matter.
     * The delimiter is auto-detected (comma or semicolon) from the header line,
     * and a leading UTF-8 BOM is stripped if present.
     *
     * @return arrid-keyed map of multimodal stop place IDs
     * @throws IOException on network errors
     */
    static Map<String, String> fetchRelations() throws IOException {
        URL url = URI.create(RELATIONS_URL).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Accept-Encoding", "gzip");
        conn.setConnectTimeout(30_000);
        conn.setReadTimeout(120_000);

        Map<String, String> result = new HashMap<>();
        try {
            InputStream raw = conn.getInputStream();
            String encoding = conn.getContentEncoding();
            InputStream is = (encoding != null && encoding.equalsIgnoreCase("gzip"))
                    ? new java.util.zip.GZIPInputStream(raw)
                    : raw;
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

            String headerLine = reader.readLine();
            if (headerLine == null) return result;

            // Strip UTF-8 BOM if present
            if (headerLine.startsWith("﻿")) {
                headerLine = headerLine.substring(1);
            }

            // Auto-detect delimiter: count occurrences of ';' and ',' in the header
            long semicolons = headerLine.chars().filter(c -> c == ';').count();
            long commas = headerLine.chars().filter(c -> c == ',').count();
            char delimiter = semicolons > commas ? ';' : ',';

            String[] headers = splitCsv(headerLine, delimiter);
            int arridIdx = -1, zdcidIdx = -1;
            for (int i = 0; i < headers.length; i++) {
                String h = headers[i].trim().toLowerCase(java.util.Locale.ROOT);
                if ("arrid".equals(h)) arridIdx = i;
                if ("zdcid".equals(h)) zdcidIdx = i;
            }
            if (arridIdx < 0 || zdcidIdx < 0) {
                logger.warn("relations.csv: could not find arrid ({}) or zdcid ({}) column in header: {}",
                        arridIdx, zdcidIdx, headerLine);
                logger.warn("Detected delimiter: '{}', columns found: {}", delimiter,
                        String.join(", ", headers));
                return result;
            }

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] fields = splitCsv(line, delimiter);
                if (fields.length <= Math.max(arridIdx, zdcidIdx)) continue;
                String arrid = fields[arridIdx].trim();
                String zdcid = fields[zdcidIdx].trim();
                if (!arrid.isEmpty() && !zdcid.isEmpty()) {
                    result.put(arrid, zdcid);
                }
            }
        } finally {
            conn.disconnect();
        }
        return result;
    }

    /**
     * Splits a CSV line using the given delimiter character, respecting
     * double-quoted fields.
     */
    private static String[] splitCsv(String line, char delimiter) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    /**
     * Convenience method: returns a simple {@code arrid → publiccode} map
     * (only entries whose {@code publiccode} is a valid platform code).
     *
     * @return filtered platform codes map
     * @throws IOException on network errors
     */
    static Map<String, String> fetchPlatformCodes() throws IOException {
        Map<String, JsonNode> data = fetchArretData();
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, JsonNode> e : data.entrySet()) {
            String publiccode = e.getValue().path("publiccode").asText(null);
            if (publiccode != null && !isStationName(publiccode)) {
                result.put(e.getKey(), publiccode);
            }
        }
        return result;
    }

    /**
     * Returns {@code true} if {@code code} looks like a station name rather
     * than a platform code.
     *
     * <ul>
     *   <li>null, empty, or {@code "-"} → true</li>
     *   <li>longer than 6 chars AND contains 4+ consecutive letters → true</li>
     * </ul>
     */
    static boolean isStationName(String code) {
        if (code == null || code.isEmpty() || "-".equals(code)) return true;
        if (code.length() > 6) {
            return STATION_NAME_LETTERS.matcher(code).find();
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // ZIP rewriting
    // -------------------------------------------------------------------------

    private static void rewriteZip(
            String inputZipPath, String outputZipPath,
            Map<String, JsonNode> arretData,
            Map<String, String> arridToZdcid) throws IOException {

        Path inputPath = Paths.get(inputZipPath);
        Path outputPath = Paths.get(outputZipPath);

        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(inputPath));
             ZipOutputStream zout = new ZipOutputStream(Files.newOutputStream(outputPath))) {

            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                zout.putNextEntry(new ZipEntry(entry.getName()));

                if ("stops.txt".equals(entry.getName())) {
                    byte[] enriched = enrichStopsTxt(zin, arretData, arridToZdcid);
                    zout.write(enriched);
                } else {
                    zin.transferTo(zout);
                }

                zout.closeEntry();
                zin.closeEntry();
            }
        }
    }

    /**
     * Reads {@code stops.txt} from {@code inputStream}, fills
     * {@code platform_code} for existing quay stops, appends new rows for
     * missing quays, and returns the enriched bytes.
     */
    private static byte[] enrichStopsTxt(
            InputStream inputStream,
            Map<String, JsonNode> arretData,
            Map<String, String> arridToZdcid) throws IOException {

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();

        // --- Parse header ---
        String headerLine = reader.readLine();
        if (headerLine == null) return new byte[0];

        String[] headers = parseCsvLine(headerLine);
        int stopIdIdx = -1, platformCodeIdx = -1;
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim();
            if ("stop_id".equals(h)) stopIdIdx = i;
            if ("platform_code".equals(h)) platformCodeIdx = i;
        }

        boolean columnExists = platformCodeIdx >= 0;
        // Normalised header for output (without BOM or stray spaces)
        String[] normalizedHeaders = Arrays.copyOf(headers, headers.length);
        for (int i = 0; i < normalizedHeaders.length; i++) {
            normalizedHeaders[i] = normalizedHeaders[i].trim();
        }

        if (columnExists) {
            sb.append(headerLine).append("\n");
        } else {
            sb.append(headerLine).append(",platform_code\n");
            // Extend normalised header for new row builder
            normalizedHeaders = Arrays.copyOf(normalizedHeaders, normalizedHeaders.length + 1);
            normalizedHeaders[normalizedHeaders.length - 1] = "platform_code";
            platformCodeIdx = normalizedHeaders.length - 1;
        }

        if (stopIdIdx < 0) {
            String line;
            while ((line = reader.readLine()) != null) sb.append(line).append("\n");
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        // --- Process existing rows ---
        Set<String> existingArrids = new HashSet<>();
        int enrichedCount = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;

            String[] fields = parseCsvLine(line);
            String stopId = stopIdIdx < fields.length ? fields[stopIdIdx].trim() : "";

            Matcher m = PURE_NUMERIC_STOP_ID.matcher(stopId);
            if (m.matches()) {
                String arrid = m.group(1);
                existingArrids.add(arrid);

                JsonNode entry = arretData.get(arrid);
                String platformCode = (entry != null)
                        ? entry.path("publiccode").asText("")
                        : "";
                if (isStationName(platformCode)) platformCode = "";

                if (!platformCode.isEmpty() && platformCodeIdx < fields.length) {
                    fields[platformCodeIdx] = platformCode;
                    sb.append(joinCsvLine(fields)).append("\n");
                    enrichedCount++;
                    continue;
                }
            }
            sb.append(line).append("\n");
        }

        // --- Append new rows for missing quays ---
        int createdCount = 0;
        for (Map.Entry<String, JsonNode> e : arretData.entrySet()) {
            String arrid = e.getKey();
            if (existingArrids.contains(arrid)) continue;

            String zdcid = arridToZdcid.get(arrid);
            if (zdcid == null || zdcid.isBlank()) continue;

            JsonNode arret = e.getValue();
            JsonNode geoPoint = arret.path("artgeopoint");
            if (geoPoint.isMissingNode()) continue;

            String lat = geoPoint.path("lat").asText(null);
            String lon = geoPoint.path("lon").asText(null);
            if (lat == null || lon == null) continue;

            String name = arret.path("artname").asText("");
            String fareZone = arret.path("artfarezone").asText("");
            String publiccode = arret.path("publiccode").asText("");
            if (isStationName(publiccode)) publiccode = "";

            String newRow = buildNewStopRow(
                    normalizedHeaders, platformCodeIdx,
                    "IDFM:" + arrid, name, lat, lon,
                    fareZone, "IDFM:" + zdcid, publiccode);
            sb.append(newRow).append("\n");
            createdCount++;
        }

        logger.info("stops.txt: enriched {} stop(s), created {} missing stop(s)",
                enrichedCount, createdCount);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Builds a CSV row for a new stop, mapping known values to the correct
     * column positions based on the actual header.
     */
    private static String buildNewStopRow(
            String[] headers, int platformCodeIdx,
            String stopId, String stopName,
            String stopLat, String stopLon,
            String zoneId, String parentStation,
            String platformCode) {

        String[] fields = new String[headers.length];
        Arrays.fill(fields, "");

        for (int i = 0; i < headers.length; i++) {
            switch (headers[i]) {
                case "stop_id"            -> fields[i] = stopId;
                case "stop_name"          -> fields[i] = stopName;
                case "stop_lat"           -> fields[i] = stopLat;
                case "stop_lon"           -> fields[i] = stopLon;
                case "zone_id"            -> fields[i] = zoneId;
                case "location_type"      -> fields[i] = "0";
                case "parent_station"     -> fields[i] = parentStation;
                case "stop_timezone"      -> fields[i] = "Europe/Paris";
                case "wheelchair_boarding"-> fields[i] = "0";
                case "platform_code"      -> fields[i] = platformCode;
                default                   -> {} // stop_desc, stop_url, etc. → ""
            }
        }
        return joinCsvLine(fields);
    }

    // -------------------------------------------------------------------------
    // Minimal CSV helpers
    // -------------------------------------------------------------------------

    /**
     * Splits a single CSV line into fields, respecting double-quoted values.
     */
    static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    /**
     * Joins fields back into a CSV line, quoting any field that contains a
     * comma, double-quote, or newline.
     */
    static String joinCsvLine(String[] fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(',');
            String f = fields[i];
            if (f.contains(",") || f.contains("\"") || f.contains("\n")) {
                sb.append('"').append(f.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(f);
            }
        }
        return sb.toString();
    }
}
