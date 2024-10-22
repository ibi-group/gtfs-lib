package com.conveyal.gtfs.dto;

public class CalendarDateDTO {
    public Integer id;
    public String service_id;
    public String date;
    public Integer exception_type;

    public static CalendarDateDTO create(String serviceId, String date, Integer exceptionType) {
        CalendarDateDTO calenderDate = new CalendarDateDTO();
        calenderDate.date = date;
        calenderDate.service_id = serviceId;
        calenderDate.exception_type = exceptionType;
        return calenderDate;
    }
}
