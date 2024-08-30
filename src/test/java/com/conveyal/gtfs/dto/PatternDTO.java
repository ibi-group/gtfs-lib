package com.conveyal.gtfs.dto;

public class PatternDTO {
    public Integer id;
    public String pattern_id;
    public String shape_id;
    public String route_id;
    public Integer direction_id;
    public Integer use_frequency;
    public String name;
    public PatternStopWithFlexDTO[] pattern_stops;
    public ShapePointDTO[] shapes;

    public static PatternDTO create(
        String routeId,
        String patternId,
        String name,
        String shapeId,
        ShapePointDTO[] shapes,
        PatternStopWithFlexDTO[] patternStops,
        int useFrequency
    ) {
        PatternDTO patternDTO = new PatternDTO();
        patternDTO.pattern_id = patternId;
        patternDTO.route_id = routeId;
        patternDTO.name = name;
        patternDTO.use_frequency = useFrequency;
        patternDTO.shape_id = shapeId;
        patternDTO.shapes = shapes;
        patternDTO.pattern_stops = patternStops;
        return patternDTO;
    }

    public static PatternDTO create(String routeId, String patternId, String name, String shapeId) {
        PatternDTO patternDTO = new PatternDTO();
        patternDTO.pattern_id = patternId;
        patternDTO.route_id = routeId;
        patternDTO.name = name;
        patternDTO.use_frequency = 1;
        patternDTO.shape_id = shapeId;
        patternDTO.shapes = new ShapePointDTO[]{
            new ShapePointDTO(2, 0.0, shapeId, 34.2222, -87.333, 0),
            new ShapePointDTO(2, 150.0, shapeId, 34.2233, -87.334, 1)
        };
        patternDTO.pattern_stops = new PatternStopWithFlexDTO[]{
            new PatternStopWithFlexDTO(patternId, "1", 0),
            new PatternStopWithFlexDTO(patternId, "2", 1)
        };
        return patternDTO;
    }

}