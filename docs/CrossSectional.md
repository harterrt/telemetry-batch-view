# Introduction

The `cross_sectional` dataset provides descriptive statistics for each
client_id in a 1% sample of main ping data. This dataset simplifies the
longitudinal table by replacing the longitudinal arrays with summary
statistics. This is the most useful dataset for describing our user base.

## Table of Contents

* [Aggregations](#aggregations)
* [Notable Metrics](#notable-metrics)
* [List of Columns](#list-of-columns)

# Aggregations

## Average

Modal values are identified with the `_avg` suffix.  All average values are
weighted by `session_length`.  For example, if we received three pings for one
`client_id` with:

[//]: # (TODO(harterrt) Exceptions?)

* `session_length = [10, 20]`
* `bookmarks = [1, 2]`

the average number of bookmarks (`bookmarks_avg`) would be `(1*10 + 2*20)/(10 +
20)` or ~1.666.

## Mode

Modal values are identified with the `_mode` suffix.  All modal values are
weighted by `session_length`.  For example, if we received three pings for one
`client_id` with:

* `session_length = [10, 10, 30]`
* `geo_country = ["IT", "IT", "DE"]`

the modal country (`geo_country_mode`) would be "DE"

If there are several modes, the most recently submitted modal value is chosen.
This may change in the future.

## Distinct Configurations

Columns with the `_configs` suffix represent the number of distinct
configurations for a variable.  For example, if we receive three pings for one
`client_id` with `geo_country = ["IT", "IT", "DE"]`, the number of distinct
`geo_county` configurations (`geo_country_configs`) would be 2.

# Notable Metrics

* `days_possible` - the number of days between the earliest and latest `submission_date`
* `default_pct` - the `subsession_length` weighted percentage of time `settings.is_default_browser` is true
* `addon_names_list` - an array of string values containing the names of all addons associated with a `client_id`
* `date_skew_per_ping_*` TODO
[//]: # (TODO(harterrt) : * `pages_count_*` - )

# List of Columns

|Column                              |Type            |Notes  |
|:---------------------------------- |:-------------- |:----- |
|client_id                           |varchar         |       |
|normalized_channel                  |varchar         |       |
|active_hours_total                  |double          |       |
|active_hours_0_mon                  |double          |       |
|active_hours_1_tue                  |double          |       |
|active_hours_2_wed                  |double          |       |
|active_hours_3_thu                  |double          |       |
|active_hours_4_fri                  |double          |       |
|active_hours_5_sat                  |double          |       |
|active_hours_6_sun                  |double          |       |
|geo_mode                            |varchar         |       |
|geo_configs                         |bigint          |       |
|architecture_mode                   |varchar         |       |
|fflocale_mode                       |varchar         |       |
|addon_count_foreign_avg             |double          |       |
|addon_count_foreign_configs         |bigint          |       |
|addon_count_foreign_mode            |bigint          |       |
|addon_count_avg                     |double          |       |
|addon_count_configs                 |bigint          |       |
|addon_count_mode                    |bigint          |       |
|number_of_pings                     |bigint          |       |
|bookmarks_avg                       |double          |       |
|bookmarks_max                       |bigint          |       |
|bookmarks_min                       |bigint          |       |
|cpu_count_mode                      |bigint          |       |
|channel_configs                     |bigint          |       |
|channel_mode                        |varchar         |       |
|days_active                         |bigint          |       |
|days_active_0_mon                   |bigint          |       |
|days_active_1_tue                   |bigint          |       |
|days_active_2_wed                   |bigint          |       |
|days_active_3_thu                   |bigint          |       |
|days_active_4_fri                   |bigint          |       |
|days_active_5_sat                   |bigint          |       |
|days_active_6_sun                   |bigint          |       |
|days_possible                       |bigint          |       |
|days_possible_0_mon                 |bigint          |       |
|days_possible_1_tue                 |bigint          |       |
|days_possible_2_wed                 |bigint          |       |
|days_possible_3_thu                 |bigint          |       |
|days_possible_4_fri                 |bigint          |       |
|days_possible_5_sat                 |bigint          |       |
|days_possible_6_sun                 |bigint          |       |
|default_pct                         |double          |       |
|locale_configs                      |bigint          |       |
|locale_mode                         |varchar         |       |
|version_configs                     |bigint          |       |
|version_max                         |varchar         |       |
|addon_names_list                    |array(varchar)  |       |
|main_ping_reason_num_aborted        |bigint          |       |
|main_ping_reason_num_end_of_day     |bigint          |       |
|main_ping_reason_num_env_change     |bigint          |       |
|main_ping_reason_num_shutdown       |bigint          |       |
|memory_avg                          |double          |       |
|memory_configs                      |bigint          |       |
|os_name_mode                        |varchar         |       |
|os_version_mode                     |varchar         |       |
|os_version_configs                  |bigint          |       |
|pages_count_avg                     |double          |       |
|pages_count_min                     |bigint          |       |
|pages_count_max                     |bigint          |       |
|plugins_count_avg                   |double          |       |
|plugins_count_configs               |bigint          |       |
|plugins_count_mode                  |bigint          |       |
|start_date_oldest                   |varchar         |       |
|start_date_newest                   |varchar         |       |
|subsession_length_badtimer          |bigint          |       |
|subsession_length_negative          |bigint          |       |
|subsession_length_toolong           |bigint          |       |
|previous_subsession_id_repeats      |bigint          |       |
|profile_creation_date               |varchar         |       |
|profile_subsession_counter_min      |bigint          |       |
|profile_subsession_counter_max      |bigint          |       |
|profile_subsession_counter_configs  |bigint          |       |
|search_counts_total                 |bigint          |       |
|search_default_configs              |bigint          |       |
|search_default_mode                 |varchar         |       |
|session_num_total                   |bigint          |       |
|subsession_branches                 |bigint          |       |
|date_skew_per_ping_avg              |double          |       |
|date_skew_per_ping_max              |bigint          |       |
|date_skew_per_ping_min              |bigint          |       |
