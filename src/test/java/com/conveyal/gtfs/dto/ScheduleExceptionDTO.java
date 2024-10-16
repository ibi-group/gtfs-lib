package com.conveyal.gtfs.dto;

public class ScheduleExceptionDTO {
    public String[] added_service;
    public String[] custom_schedule;
    public String[] dates;
    public Integer exemplar;
    public Integer id;
    public String name;
    public String[] removed_service;

    /** Empty constructor for deserialization */
    public ScheduleExceptionDTO() {}

    public static ScheduleExceptionDTO create() {
        ScheduleExceptionDTO scheduleExceptionDTO = new ScheduleExceptionDTO();
        scheduleExceptionDTO.name = "Halloween";
        scheduleExceptionDTO.exemplar = 9; // Add, swap, or remove type
        scheduleExceptionDTO.removed_service = new String[] {"1"};
        scheduleExceptionDTO.dates = new String[] {"20191031"};
        return scheduleExceptionDTO;
    }
}
