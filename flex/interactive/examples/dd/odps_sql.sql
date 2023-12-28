CREATE Table user_node(ding_user_id bigint, ding_user_nick string, city_last_visit string, role_name bigint, act_usr_days_1m bigint, org_industry_sub_id string);
CREATE Table org_node(org_id bigint, org_name string, org_industry_sub_id string, auth_level string);
CREATE Table edu_org_node(org_id bigint, org_name string, auth_level string);
CREATE Table group_node(group_id bigint, group_name string);
CREATE Table user_user_edge(ding_user_id bigint, friend_user_id bigint, user_intimacy_score double, communication_score_30d bigint);
CREATE Table user_group_edge(ding_user_id bigint, group_id bigint, gmt_create_mill bigint);
CREATE Table user_friend_edge(ding_user_id bigint, friend_user_id bigint, gmt_create_mill bigint);
CREATE Table org_user_edge(ding_user_id bigint, org_id bigint, gmt_create_mill bigint, staff_lst_leave_time_mill bigint, role_name bigint);
CREATE Table edu_org_user_edge(ding_user_id bigint, org_id bigint, gmt_create_mill bigint, staff_lst_leave_time_mill bigint, profession string);
