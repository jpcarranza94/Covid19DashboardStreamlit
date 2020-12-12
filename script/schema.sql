CREATE TABLE covid.confirmed(
    id int primary key auto_increment,
    country_region varchar(50),
    province_state varchar(50),
    lat decimal(10,7),
    `long` decimal(10,7),
    event_date datetime(6),
    cases int,
    cases_per_day int,
    cum_sum_by_country int,
    cum_max int,
    cases_per_day_per_country int
);



CREATE TABLE covid.deaths(
    id int primary key auto_increment,
    country_region varchar(50),
    province_state varchar(50),
    lat decimal(10,7),
    `long` decimal(10,7),
    event_date datetime(6),
    cases int,
    cases_per_day int,
    cum_sum_by_country int,
    cum_max int,
    cases_per_day_per_country int
);

CREATE TABLE covid.recovered(
    id int primary key auto_increment,
    country_region varchar(50),
    province_state varchar(50),
    lat decimal(10,7),
    `long` decimal(10,7),
    event_date datetime(6),
    cases int,
    cases_per_day int,
    cum_sum_by_country int,
    cum_max int,
    cases_per_day_per_country int
);

CREATE TABLE covid.cases_data(
    id int primary key auto_increment,
    country_region varchar(50),
    province_state varchar(50),
    lat decimal(10,7),
    `long` decimal(10,7),
    event_date datetime(6),
    c_cases int,
    r_cases int,
    d_cases int,
    c_cases_per_day int,
    r_cases_per_day int,
    d_cases_per_day int,
    c_cum_sum_by_country int,
    r_cum_sum_by_country int,
    d_cum_sum_by_country int,
    c_cum_max int,
    r_cum_max int,
    d_cum_max int,
    c_cases_per_day_per_country int,
    r_cases_per_day_per_country int,
    d_cases_per_day_per_country int,
    mortality_rate decimal(10,7),
    recovery_rate decimal(10,7)
);

