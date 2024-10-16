package com.conveyal.gtfs.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeedInfoDTO {
    public int id;
    public String feed_id;
    public String feed_publisher_name;
    public String feed_publisher_url;
    public String feed_lang;
    public String feed_start_date;
    public String feed_end_date;
    public String feed_version;
    public String default_route_color;
    public String default_route_type;
    public String default_lang;
    public String feed_contact_email;
    public String feed_contact_url;

    public static FeedInfoDTO create() {
        FeedInfoDTO feedInfoDTO = new FeedInfoDTO();
        feedInfoDTO.feed_id = "fake_id";
        feedInfoDTO.feed_publisher_name = "' OR 1 = 1; SELECT '1";
        feedInfoDTO.feed_publisher_url = "example.com";
        feedInfoDTO.feed_lang = "en";
        feedInfoDTO.feed_start_date = "07052021";
        feedInfoDTO.feed_end_date = "09052021";
        feedInfoDTO.feed_lang = "en";
        feedInfoDTO.default_route_color = "1c8edb";
        feedInfoDTO.default_route_type = "3";
        feedInfoDTO.default_lang = "en";
        feedInfoDTO.feed_contact_email = "a@b.com";
        feedInfoDTO.feed_contact_url = "example.com";
        return feedInfoDTO;
    }
}
