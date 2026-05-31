package org.jouca.idfm_gtfs_rt.fetchers;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

import org.slf4j.*;

/**
 * Enriches a GTFS ZIP with IDFM fare data (GTFS Fares V2).
 *
 * <p>Files generated:
 * <ul>
 *   <li>{@code fare_products.txt} — the five IDFM fare tiers.</li>
 *   <li>{@code networks.txt} — one network per fare category.</li>
 *   <li>{@code route_networks.txt} — each route assigned to its network.</li>
 *   <li>{@code areas.txt} — {@code zone_airport} and {@code zone_default}.</li>
 *   <li>{@code stop_areas.txt} — airport stops assigned to {@code zone_airport}.</li>
 *   <li>{@code fare_leg_rules.txt} — network + zone → fare product mapping.</li>
 * </ul>
 *
 * <p>Any Fares V1 remnants ({@code fare_attributes.txt}, {@code fare_rules.txt})
 * are dropped from the output ZIP.
 *
 * <p>Route colours/names are also updated for CDG VAL, ORLYVAL and magical shuttles.
 */
public class FareEnricher {

    private static final Logger logger = LoggerFactory.getLogger(FareEnricher.class);

    private static final Set<String> AIRPORT_STOP_IDS = Set.of(
        "IDFM:monomodalStopPlace:462398",
        "IDFM:monomodalStopPlace:473364",
        "IDFM:490908",
        "IDFM:490917"
    );

    private static final Map<String, String> MAGICAL_SHUTTLE_NAMES = Map.of(
        "Selected", "Villages Nature",
        "Orly",     "Orly",
        "CDG",      "CDG"
    );

    private static final Set<String> TRAM_SHORT_NAMES = Set.of(
        "T1", "T2", "T3a", "T3b", "T4", "T5", "T6", "T7", "T8", "T9", "T10"
    );

    private static final String NET_BUS_TRAM  = "network_bus_tram";
    private static final String NET_METRO_RER = "network_metro_train_rer";
    private static final String NET_AIRPORT   = "network_airport_express";
    private static final String NET_FREE      = "network_free";
    private static final String NET_MAGICAL   = "network_magical_shuttle";

    /** V1 files to drop + V2 files to regenerate — all skipped on the copy pass. */
    private static final Set<String> MANAGED_ENTRIES = Set.of(
        "fare_attributes.txt", "fare_rules.txt",
        "fare_products.txt", "networks.txt", "route_networks.txt",
        "areas.txt", "stop_areas.txt", "fare_leg_rules.txt"
    );

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public static void addFares(String inputZipPath, String outputZipPath) throws IOException {
        logger.info("Starting Fares V2 enrichment…");
        rewriteZipWithFares(inputZipPath, outputZipPath);
        logger.info("Fares V2 enriched GTFS written to {}", outputZipPath);
    }

    // -------------------------------------------------------------------------
    // ZIP rewriting
    // -------------------------------------------------------------------------

    private static void rewriteZipWithFares(String inputZipPath, String outputZipPath)
            throws IOException {

        Path inputPath  = Paths.get(inputZipPath);
        Path outputPath = Paths.get(outputZipPath);

        // First pass: read routes.txt to derive networks and route colours
        byte[] routesBytes = null;
        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(inputPath))) {
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if ("routes.txt".equals(entry.getName())) routesBytes = zin.readAllBytes();
                zin.closeEntry();
            }
        }

        if (routesBytes == null) {
            logger.warn("No routes.txt found in GTFS ZIP; skipping fare enrichment");
            if (!inputZipPath.equals(outputZipPath))
                Files.copy(inputPath, outputPath, StandardCopyOption.REPLACE_EXISTING);
            return;
        }

        ParsedRoutes parsed = parseAndUpdateRoutes(routesBytes);

        // Second pass: copy unchanged entries, inject enriched routes and new fare files
        try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(inputPath));
             ZipOutputStream zout = new ZipOutputStream(Files.newOutputStream(outputPath))) {

            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                String name = entry.getName();
                if (MANAGED_ENTRIES.contains(name)) {
                    zin.closeEntry();
                    continue;
                }
                zout.putNextEntry(new ZipEntry(name));
                if ("routes.txt".equals(name)) {
                    zout.write(parsed.enrichedBytes());
                } else {
                    zin.transferTo(zout);
                }
                zout.closeEntry();
                zin.closeEntry();
            }

            writeEntry(zout, "fare_products.txt",  buildFareProducts());
            writeEntry(zout, "networks.txt",        buildNetworks());
            writeEntry(zout, "route_networks.txt",  buildRouteNetworks(parsed.routeNetworkLines()));
            writeEntry(zout, "areas.txt",           buildAreas());
            writeEntry(zout, "stop_areas.txt",      buildStopAreas());
            writeEntry(zout, "fare_leg_rules.txt",  buildFareLegRules());
        }

        logger.info("Assigned {} routes to networks", parsed.routeNetworkLines().size());
    }

    // -------------------------------------------------------------------------
    // routes.txt processing
    // -------------------------------------------------------------------------

    private record ParsedRoutes(byte[] enrichedBytes, List<String> routeNetworkLines) {}

    private static ParsedRoutes parseAndUpdateRoutes(byte[] routesBytes) throws IOException {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream(routesBytes), StandardCharsets.UTF_8));

        String headerLine = reader.readLine();
        if (headerLine == null) return new ParsedRoutes(routesBytes, List.of());

        String[] headers = GTFSEnricher.parseCsvLine(headerLine);
        int routeIdIdx = -1, routeTypeIdx = -1, shortNameIdx = -1,
            colorIdx   = -1, textColorIdx  = -1;

        for (int i = 0; i < headers.length; i++) {
            switch (headers[i].trim()) {
                case "route_id"         -> routeIdIdx    = i;
                case "route_type"       -> routeTypeIdx  = i;
                case "route_short_name" -> shortNameIdx  = i;
                case "route_color"      -> colorIdx      = i;
                case "route_text_color" -> textColorIdx  = i;
            }
        }

        StringBuilder sb = new StringBuilder(headerLine).append("\n");
        List<String> routeNetworkLines = new ArrayList<>();

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;
            String[] fields = GTFSEnricher.parseCsvLine(line);

            String routeId   = field(fields, routeIdIdx);
            String routeType = field(fields, routeTypeIdx);
            String shortName = field(fields, shortNameIdx);

            if (!routeId.isEmpty())
                routeNetworkLines.add(routeId + "," + determineNetwork(shortName, routeType));

            // Colour and name overrides
            if (MAGICAL_SHUTTLE_NAMES.containsKey(shortName)) {
                setField(fields, shortNameIdx, MAGICAL_SHUTTLE_NAMES.get(shortName));
                setField(fields, colorIdx,     "EB212D");
                setField(fields, textColorIdx, "FFFFFF");
            } else if ("CDG VAL".equals(shortName)) {
                setField(fields, colorIdx,     "1857B6");
                setField(fields, textColorIdx, "FFFFFF");
            } else if ("ORLYVAL".equals(shortName)) {
                setField(fields, colorIdx,     "2E4D5C");
                setField(fields, textColorIdx, "FFFFFF");
            }

            sb.append(GTFSEnricher.joinCsvLine(fields)).append("\n");
        }

        return new ParsedRoutes(sb.toString().getBytes(StandardCharsets.UTF_8), routeNetworkLines);
    }

    private static String determineNetwork(String shortName, String routeType) {
        if ("CDG VAL".equals(shortName) || "N1".equals(shortName) || "N2".equals(shortName))
            return NET_FREE;
        if (MAGICAL_SHUTTLE_NAMES.containsKey(shortName))
            return NET_MAGICAL;
        if ("ROISSYBUS".equals(shortName) || "ORLYVAL".equals(shortName))
            return NET_AIRPORT;
        if ("3".equals(routeType) || TRAM_SHORT_NAMES.contains(shortName))
            return NET_BUS_TRAM;
        return NET_METRO_RER;
    }

    // -------------------------------------------------------------------------
    // Fares V2 file builders
    // -------------------------------------------------------------------------

    private static byte[] buildFareProducts() {
        return csv(
            "fare_product_id,fare_product_name,amount,currency",
            "ticket_bus_tram,Ticket Bus/Tram,2.05,EUR",
            "ticket_metro_train_rer,Ticket Métro/Train/RER,2.55,EUR",
            "ticket_airport,Ticket Aéroport,14.00,EUR",
            "ticket_free,Gratuit,0.00,EUR",
            "ticket_magical_shuttle,Navette Villages Nature,24.00,EUR"
        );
    }

    private static byte[] buildNetworks() {
        return csv(
            "network_id,network_name",
            NET_BUS_TRAM  + ",Bus et Tram",
            NET_METRO_RER + ",Métro / Train / RER",
            NET_AIRPORT   + ",Express Aéroport",
            NET_FREE      + ",Gratuit",
            NET_MAGICAL   + ",Navette Villages Nature"
        );
    }

    private static byte[] buildRouteNetworks(List<String> routeNetworkLines) {
        StringBuilder sb = new StringBuilder("route_id,network_id\n");
        for (String line : routeNetworkLines) sb.append(line).append("\n");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] buildAreas() {
        return csv(
            "area_id,area_name",
            "zone_airport,Zone Aéroport",
            "zone_default,Zone Standard"
        );
    }

    private static byte[] buildStopAreas() {
        StringBuilder sb = new StringBuilder("area_id,stop_id\n");
        for (String stopId : AIRPORT_STOP_IDS)
            sb.append("zone_airport,").append(stopId).append("\n");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Maps networks and fare zones to fare products.
     *
     * <p>For metro/train/RER, two rules with different {@code rule_priority} values
     * handle the airport surcharge: the higher-priority airport rules (1) override
     * the default (0) when the origin or destination stop is in {@code zone_airport}.
     */
    private static byte[] buildFareLegRules() {
        return csv(
            "leg_group_id,network_id,from_area_id,to_area_id,fare_product_id,rule_priority",
            "bus_tram,"           + NET_BUS_TRAM  + ",,,ticket_bus_tram,0",
            "metro_default,"      + NET_METRO_RER + ",,,ticket_metro_train_rer,0",
            "metro_from_airport," + NET_METRO_RER + ",zone_airport,,ticket_airport,1",
            "metro_to_airport,"   + NET_METRO_RER + ",,zone_airport,ticket_airport,1",
            "airport_express,"    + NET_AIRPORT   + ",,,ticket_airport,0",
            "free,"               + NET_FREE      + ",,,ticket_free,0",
            "magical_shuttle,"    + NET_MAGICAL   + ",,,ticket_magical_shuttle,0"
        );
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] csv(String... lines) {
        return (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8);
    }

    private static void writeEntry(ZipOutputStream zout, String name, byte[] content)
            throws IOException {
        zout.putNextEntry(new ZipEntry(name));
        zout.write(content);
        zout.closeEntry();
    }

    private static String field(String[] fields, int idx) {
        return idx >= 0 && idx < fields.length ? fields[idx].trim() : "";
    }

    private static void setField(String[] fields, int idx, String value) {
        if (idx >= 0 && idx < fields.length) fields[idx] = value;
    }
}
