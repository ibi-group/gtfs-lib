package com.conveyal.gtfs.dto;

public class CalendarDTO {
    public Integer id;
    public String service_id;
    public Integer monday;
    public Integer tuesday;
    public Integer wednesday;
    public Integer thursday;
    public Integer friday;
    public Integer saturday;
    public Integer sunday;
    public String start_date;
    public String end_date;
    public String description;

    public static CalendarDTO create(String serviceId, String startDate, String endDate) {
        CalendarDTO calendarDTO = new CalendarDTO();
        calendarDTO.service_id = serviceId;
        calendarDTO.monday = 1;
        calendarDTO.tuesday = 1;
        calendarDTO.wednesday = 1;
        calendarDTO.thursday = 1;
        calendarDTO.friday = 1;
        calendarDTO.saturday = 0;
        calendarDTO.sunday = 0;
        calendarDTO.start_date = startDate;
        calendarDTO.end_date = endDate;
        return calendarDTO;
    }

}
