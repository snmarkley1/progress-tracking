#########################################################################
#########################################################################
###                                                                   ###
###                   Progress Tracking: Geospatial                   ###
###                                                                   ###
#########################################################################
#########################################################################

## ONLY NEED TO RUN THIS SCRIPT ONCE

## -----------------------------------------------------
## Prepare workspace
## -----------------------------------------------------

source("scripts/00_preamble.R")


## -------------------------------------------------------
## Import population data
## -------------------------------------------------------

bg_centroids <- st_read(
  paste(
    getwd() |>
      dirname(),
  "jurisdiction-population-estimate",
  "data",
  "bg_centroids.gpkg",
  sep = "/"
  )
) |>
  print()

## -------------------------------------------------------
## Import data from Editor
## -------------------------------------------------------


## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##
## !!!!!!  Need to be logged into VPN   !!!!!!! ##
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##


## set connection
## -----------------------------------------------------

conn <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("DB_HOST"),
  port = Sys.getenv("DB_PORT"),
  dbname = Sys.getenv("DBNAME"),
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD"),
  sslmode = "require"
)

## check connection
# pgPostGIS(conn) # rpostgis package

## list geometries
# pgListGeom(conn, geog = TRUE) # rpostgis package


## -----------------------------------------------------------------
## import jursidiction by GIS analyst info
## -----------------------------------------------------------------

## set query
query <- paste0(
  "
  WITH
	A AS (
		SELECT
			jurisdiction_id,
			COUNT(id) AS mapped_base_districts
		FROM
			website_jurisdictionzone
		WHERE
		  website_jurisdictionzone.overlay is FALSE AND website_jurisdictionzone.mapped is TRUE
		GROUP BY
			jurisdiction_id
	),
	B AS (
	  SELECT
	    jurisdiction_id,
	    COUNT(id) AS mapped_overlay_districts
	  FROM
	    website_jurisdictionzone
	  WHERE
	    website_jurisdictionzone.overlay IS TRUE AND website_jurisdictionzone.mapped is TRUE
	  GROUP BY
	    jurisdiction_id
	),
	C AS (
	  SELECT
	    jurisdiction_id,
	    COUNT(id) AS unmapped_districts
	  FROM
	    website_jurisdictionzone
	  WHERE
	    website_jurisdictionzone.mapped is FALSE
	  GROUP BY
	    jurisdiction_id
	),
	D AS (
	  SELECT
	    jurisdiction_id,
	    COUNT(id) AS submitted_districts
	  FROM
	    website_jurisdictionzone
	  WHERE
	    website_jurisdictionzone.status IN ('done', 'review')
	  GROUP BY
	    jurisdiction_id
	),
	E AS (
    SELECT
      jurisdiction_id,
      COUNT(id) AS geometry_added
    FROM
      website_jurisdictionzone
    WHERE
      website_jurisdictionzone.geom IS NOT NULL
    GROUP BY
      jurisdiction_id
  )
  SELECT
    website_jurisdiction.name AS jurisdiction,
    website_county.name AS county,
    website_state.name AS state,
    website_jurisdiction.haszoning,
    website_jurisdiction.published,
    website_jurisdiction.publish_date,
    website_jurisdiction.zoningcodepages,
    ROUND(website_jurisdiction.acres) AS acres,
    website_jurisdiction.percentcompleted,
    website_jurisdiction.reviewstatus,
    website_jurisdiction.updated,
    gis_user.id AS gis_id,
    gis_user.first_name || ' ' || gis_user.last_name AS gis_name,
    gis_user.date_joined::date AS gis_date_joined,
    SPLIT_PART(gis_user.email, '@', 2) IN ('landuseatlas.org', 'cornell.edu') AS gis_is_staff,
    gis_user.email AS gis_email,
    zoning_user.id AS zoning_id,
    zoning_user.first_name || ' ' || zoning_user.last_name AS zoning_name,
    zoning_user.date_joined::date AS zoning_date_joined,
    SPLIT_PART(zoning_user.email, '@', 2) IN ('landuseatlas.org', 'cornell.edu') AS zoning_is_staff,
    zoning_user.email AS zoning_email,
    A.mapped_base_districts,
	  COALESCE(B.mapped_overlay_districts, 0) AS mapped_overlay_districts,
	  COALESCE(C.unmapped_districts, 0) AS unmapped_districts,
	  COALESCE(D.submitted_districts, 0) AS submitted_districts,
	  COALESCE(E.geometry_added, 0) AS geometry_added
  FROM
    A
	  LEFT JOIN B ON B.jurisdiction_id = A.jurisdiction_id
	  LEFT JOIN C ON C.jurisdiction_id = A.jurisdiction_id
	  LEFT JOIN D ON D.jurisdiction_id = A.jurisdiction_id
	  LEFT JOIN E ON E.jurisdiction_id = A.jurisdiction_id
	  JOIN website_jurisdiction ON website_jurisdiction.id = A.jurisdiction_id
    JOIN website_county ON website_jurisdiction.county_id = website_county.id
    JOIN website_state ON website_county.state_id = website_state.id
    -- Join assigned GIS analyst details
    LEFT JOIN website_teammembership AS gis_member ON gis_member.id = website_jurisdiction.assignedto2_id
    LEFT JOIN auth_user AS gis_user ON gis_user.id = gis_member.member_id
    -- Join assigned zoning analyst details
    LEFT JOIN website_teammembership AS zoning_member ON zoning_member.id = website_jurisdiction.assignedto_id
    LEFT JOIN auth_user AS zoning_user ON zoning_user.id = zoning_member.member_id
  WHERE
    haszoning
    -- AND (
    --    SPLIT_PART(zoning_user.email, '@', 2) IN ('landuseatlas.org', 'cornell.edu')
    --    OR SPLIT_PART(gis_user.email, '@', 2) IN ('landuseatlas.org', 'cornell.edu')
    -- )
  "
)


## import
juris_table_import <- dbGetQuery(conn, query) |>
  #select(jurisdiction, county, state, published:assignedto2_id) |>
  #filter(!is.na(assignedto2_id) & haszoning == TRUE) |>
  as_tibble() |>
  arrange(jurisdiction, county, state) |>
  mutate(total_districts = mapped_base_districts + mapped_overlay_districts + unmapped_districts) |>
  print()

  
  ## -------------------------------------------------------------------
  ## set query and pull zoning district data
  ## ------------------------------------------------------------------
  
  ## set query
query_geo <- paste0(
  "
  SELECT
    a.name as jurisdiction,
    a.haszoning,
    a.published,
    a.reviewstatus,
    a.geom,
    website_county.name as county,
    website_state.name as state
  FROM website_jurisdiction a
  JOIN website_county on a.county_id = website_county.id
  JOIN website_state on website_county.state_id = website_state.id
  WHERE
    a.haszoning is TRUE
  "
)
  
  
## import using query
juris_geo_import <- st_read(conn, query = query_geo) %>%
  # remove empty polygons
  filter(!st_is_empty(.)) |>
  # keep only what's in staff ones from above
  filter(
    paste(state, county, jurisdiction, sep = "_") %in%
      paste(juris_table_import$state, juris_table_import$county, juris_table_import$jurisdiction, sep = "_")
  ) |>
  print()


## juris population
juris_pop <- bg_centroids |>
  st_intersection(juris_geo_import) |>
  st_drop_geometry() |>
  group_by(jurisdiction, county, state) |>
  summarize(population = sum(population)) |>
  ungroup() |>
  print()
  


## -------------------------------------------------------------------
## Combine population stats with Editor stats
## -------------------------------------------------------------------

juris_combined <- juris_table_import |>
  left_join(juris_pop, by = c("jurisdiction", "county", "state")) |>
  select(jurisdiction:published, publish_date, updated, reviewstatus, geometry_added, zoningcodepages, acres, population, mapped_base_districts:total_districts, gis_name, gis_is_staff, gis_date_joined, gis_email, zoning_name, zoning_is_staff, zoning_date_joined, zoning_email) |>
  mutate(population = ifelse(is.na(population), 0, population)) |>
  print()
  


## -------------------------------------------------------------------
## check total jurisdiction county by GIS analyst
## -------------------------------------------------------------------

gis_table_group <- juris_combined |>
  #filter(reviewstatus %in% c("zoning", "giszoning") | published == TRUE) |>
  filter(!(gis_name == "Tara Safavian" & state == "California")) |>
  filter(
    gis_name %in%
      c(
        "Aidan Antonienko",
        "Coral Troxell",
        "Conor Nolan",
        "Melissa Hayashida",
        "Patrick DeHoyos",
        "Dajoin Williams",
        "Tara Safavian"
      )
  ) |>
  mutate(
    team = ifelse(str_detect(gis_email, "cornell"), "cornell", "luai"),
    jurisdiction_geo_added = ifelse(geometry_added > 0 | reviewstatus %in% c("zoning", "giszoning") | published == TRUE, 1, 0),
    geo_completed = ifelse(reviewstatus %in% c("zoning", "giszoning") | published == TRUE, 1, 0),
    gis_date_joined = ifelse(gis_name == "Tara Safavian", as.Date("2024-03-25"), gis_date_joined),
    gis_date_joined = as.Date(gis_date_joined)
  ) |>
  filter(jurisdiction_geo_added == 1) |>
  group_by(gis_name, gis_date_joined, team) |>
  summarize(
    zoningcodepages = mean(zoningcodepages, na.rm = T),
    mean_acres = mean(acres),
    mean_population = mean(population),
    mean_districts = mean(total_districts),
    mean_mapped_overlay_districts = mean(mapped_overlay_districts),
    district_geo_added = sum(geometry_added),
    jurisdiction_geo_added = sum(jurisdiction_geo_added),
    jurisdiction_geo_completed = sum(geo_completed),
    total_jurisdiction_count = n()
  ) |>
  ungroup() |>
  # add additional fields
  mutate(
    strand_cleaning =
      case_when(
        str_detect(gis_name, "Aidan") ~ 90,
        str_detect(gis_name, "Conor") ~ 103,
        str_detect(gis_name, "Coral") ~ 156,
        str_detect(gis_name, "Melissa") ~ 47,
        TRUE ~ 0
      ),
    overlay_adjustments = ifelse(str_detect(gis_name, "Aidan"), 93, 0),
    zoning_analysis = ifelse(gis_name %in% c("Patrick DeHoyos", "Tara Safavian"), "yes", "no"),
    holidays =
      case_when(
        str_detect(gis_name, "Aidan") ~ 16,
        str_detect(gis_name, "Coral|Conor|Melissa") ~ 12,
        TRUE ~ 3
      ),
    pto =
      case_when(
        str_detect(gis_name, "Aidan") ~ 148,
        str_detect(gis_name, "Coral") ~ 70.5,
        str_detect(gis_name, "Conor") ~ 111.5,
        str_detect(gis_name, "Melissa") ~ 50.5,
        TRUE ~ 0
      ),
    workdays = (Sys.Date() - gis_date_joined) * 5 / 7 - holidays,
    workhours = ifelse(gis_name == "Melissa Hayashida", workdays * 7, workdays * 8),
    workhours = workhours - pto,
    adjusted_jurisdiction_count = jurisdiction_geo_added + 0.25 * strand_cleaning + 0.25 * overlay_adjustments,
    hours_per_juris = workhours / adjusted_jurisdiction_count
  ) |>
  #group_by(team) |>
  #mutate(avg_juris_speed_by_team = mean(hours_per_juris)) |>
  #ungroup() |>
  arrange(gis_name) |>
  select(-holidays, -pto, -zoning_analysis) |>
  print()



## ---------------------------------------------------------------------
## check total jurisdiction county by Zoning analyst
## ---------------------------------------------------------------------


zoning_table_group <- juris_combined |>
  #filter(reviewstatus %in% c("gis", "giszoning") | published == "TRUE") |>
  filter(!(zoning_name %in% c("Tara Safavian", "Anthony La") & state == "California")) |>
  filter(
    zoning_name %in% c(
      "Anthony La",
      "Ashleigh Graham-Clarke",
      "Devon Chodzin",
      "Jeff Conkle",
      "Joanna Kaufman",
      "Jonah Pellecchia",
      "Junbo Huang",
      "Olivia Ranseen",
      "Patrick DeHoyos",
      "Rama Karlapalem",
      "Tara Safavian"
    )
  ) |>
  mutate(
    team = ifelse(str_detect(zoning_email, "cornell"), "cornell", "luai"),
    finished_jurisdictions = ifelse(reviewstatus %in% c("gis", "giszoning") | published == "TRUE", 1, 0)
    ) |>
  group_by(zoning_name, zoning_date_joined, team) |>
  summarize(
    zoning_date_joined = min(zoning_date_joined),
    zoningcodepages = sum(zoningcodepages, na.rm = T),
    #acres = sum(acres),
    mean_population = mean(population),
    #total_districts = sum(total_districts),
    submitted_districts = sum(submitted_districts),
    jurisdiction_count = sum(finished_jurisdictions)
  ) |>
  mutate(
    zoning_end_date = ifelse(n() > 1, date(max(zoning_date_joined)), Sys.Date()),
    zoning_end_date = ifelse(zoning_date_joined == max(zoning_end_date), Sys.Date(), zoning_end_date),
    zoning_end_date = as.Date(zoning_end_date)
  ) |>
  ungroup() |>
  mutate(
    # holidays =
    #   case_when(
    #     zoning_date_joined, 
    #     str_detect(gis_name, "Joanna") ~ 16,
    #     str_detect(gis_name, "Coral|Conor|Melissa") ~ 12,
    #     TRUE ~ 3
    #   ),
    # pto =
    #   case_when(
    #     str_detect(zoning_name, "Jonah" & team == "cornell") ~ 126,
    #     str_detect(zoning_name, "Coral") ~ 70.5,
    #     str_detect(gis_name, "Conor") ~ 111.5,
    #     str_detect(gis_name, "Melissa") ~ 50.5,
    #     TRUE ~ 0
    #   ),
    workdays = as.numeric(zoning_end_date - zoning_date_joined) * 5/7
    #workdays = (Sys.Date() - gis_date_joined) * 5 / 7 - holidays,
    #workhours = ifelse(gis_name == "Melissa Hayashida", workdays * 35, workdays * 40),
    #workhours = workhours - pto,
    #adjusted_jurisdiction_count = jurisdiction_count + 0.25 * strand_cleaning + 0.25 * overlay_adjustments,
    #hours_per_juris = workhours / adjusted_jurisdiction_count
    ) |>
  print()


## figure out days
test <- data.frame(
  date = seq(as.Date("2023-01-01"), as.Date("2024-12-31"), "days")) |>
  mutate(
    holiday =
      case_when(
        str_detect(as.character(date), "-01-01|-01-15") ~ 1,
        TRUE ~ 0
      )
  ) |>
  as_tibble() |>
  print()



# add additional fields
  mutate(
    strand_cleaning =
      case_when(
        str_detect(gis_name, "Aidan") ~ 90,
        str_detect(gis_name, "Conor") ~ 103,
        str_detect(gis_name, "Coral") ~ 156,
        str_detect(gis_name, "Melissa") ~ 47,
        TRUE ~ 0
      ),
    overlay_adjustments = ifelse(str_detect(gis_name, "Aidan"), 93, 0),
    zoning_analysis = ifelse(gis_name %in% c("Patrick DeHoyos", "Tara Safavian"), "yes", "no"),
    holidays =
      case_when(
        str_detect(gis_name, "Aidan") ~ 16,
        str_detect(gis_name, "Coral|Conor|Melissa") ~ 12,
        TRUE ~ 3
      ),
    pto =
      case_when(
        str_detect(gis_name, "Aidan") ~ 148,
        str_detect(gis_name, "Coral") ~ 70.5,
        str_detect(gis_name, "Conor") ~ 111.5,
        str_detect(gis_name, "Melissa") ~ 50.5,
        TRUE ~ 0
      ),
    workdays = (Sys.Date() - gis_date_joined) * 5 / 7 - holidays,
    workhours = ifelse(gis_name == "Melissa Hayashida", workdays * 35, workdays * 40),
    workhours = workhours - pto,
    adjusted_jurisdiction_count = jurisdiction_count + 0.25 * strand_cleaning + 0.25 * overlay_adjustments,
    hours_per_juris = workhours / adjusted_jurisdiction_count
  ) |>
  arrange(gis_name) |>
  select(-holidays, -pto, -zoning_analysis) |>
  print()




## set query
published_query <- paste0(
  "
  select a.name,
  a.published,
  a.govtype,
  a.haszoning,
  website_county.name as county,
  website_county.fips,
  website_state.name as state
  from website_jurisdiction a
  join website_county on a.county_id = website_county.id
  join website_state on website_county.state_id = website_state.id
  where a.published IS TRUE
  "
)

published_table_import <- dbGetQuery(conn, published_query) |>
  as_tibble() |>
  group_by(county, fips) |>
  summarize(jurisdictions = n()) |>
  print()



## -------------------------------------------------------------------
## combine
## -------------------------------------------------------------------

## combine at county level
combined_data_county <- jurisdictions_by_county_state_msa |>
  left_join(in_progress_table_import[c("fips", "jurisdictions")], by = c("COUNTYID" = "fips")) |>
  left_join(published_table_import[c("fips", "jurisdictions")], by = c("COUNTYID" = "fips"), suffix = c("_in_progress", "_published")) %>%
  mutate_at(
    vars(jurisdictions_in_progress, jurisdictions_published),
    ~ifelse(is.na(.), 0, .)
  ) |>
  rename(
    CBSA = CSA,
    CBSAID = CSAID,
    Total = total_jurisdictions,
    `In Progress` = jurisdictions_in_progress,
    Published = jurisdictions_published
  ) |>
  mutate(
    `Not Started` = Total - Published - `In Progress`,
    # fix negatives
    Total = ifelse(Total < (`In Progress` + Published), `In Progress` + Published, Total),
    `Not Started` = ifelse(`Not Started` < 0, 0, `Not Started`),
    # fix CT
    Published = ifelse(STATE == "Connecticut", Total, Published),
    `Not Started` = ifelse(STATE == "Connecticut", 0, `Not Started`)
  ) |>
  select(STATE:population, `Not Started`, `In Progress`, Published, Total) |>
  print()


## combine at CBSA level
combined_data_cbsa <- combined_data_county |>
  group_by(CBSA, CBSAID, population) |>
  summarize_at(
    vars(`Not Started`:Total),
    sum
  ) |>
  # fix CT
  ungroup() |>
  arrange(-population) |>
  mutate(
    Remaining = Total - Published,
    Rank = row_number(),
    #`% Published` = round(Published / Total * 100, 1)
  ) |>
  select(CBSA:population, Rank, `Not Started`:Remaining) |>
  rename(Population = population) |>
  print()


## -------------------------------------------------------
## Save out
## -------------------------------------------------------

date_time <- paste0(str_replace_all(str_sub(Sys.time(), 1, 10), "-", ""), "_", str_replace_all(str_sub(Sys.time(), 12, 19), ":", ""))
filename <- paste0("reports/msa_progressreport", "_", date_time, ".csv")
write_csv(combined_data_cbsa, filename)



## -------------------------------------------------------
## Save just the top 40
## -------------------------------------------------------

cbsa_top40 <- combined_data_cbsa |>
  filter(Rank <= 40) |>
  print()

filename <- paste0("reports/msa_top40_progressreport", "_", date_time, ".csv")
write_csv(cbsa_top40, filename)


## -------------------------------------------------------------------
## combine
## -------------------------------------------------------------------

# 
# 
# juris_table_prep <- juris_table_import |>
#   mutate(
#     InProgress = ifelse(published == FALSE & govtype == "", 1, 0)
#   ) |>
#   group_by(state, county, published) |>
#   summarize(
#     jurisdictions = n(),
#     InProgress = sum(InProgress)
#     ) |>
#   pivot_wider(
#     names_from = published,
#     values_from = jurisdictions
#   ) |>
#   rename(
#     Claimed = `FALSE`,
#     Published = `TRUE`
#   ) %>%
#   mutate_at(
#     vars(Claimed, Published),
#     ~ifelse(is.na(.), 0, .)
#   ) |>
#   group_by(state, county) %>%
#   summarize_at(
#     vars(InProgress:Published),
#     ~sum(.)
#   ) |>
#   ungroup() |>
#   print()
# 
# 
# 
# ## -------------------------------------------------------
# ## Combine
# ## -------------------------------------------------------
# 
# 
# msa_juris_prep <- juris_table_prep |>
#   left_join(jurisdictions_by_county_state_msa, by = c("state" = "STATE", "county" = "COUNTY")) |>
#   filter(!is.na(CSA)) |>
#   group_by(CSAID, CSA, population) |>
#   summarize(
#     Claimed = sum(Claimed),
#     `In Progress` = sum(InProgress),
#     Published = sum(Published),
#     Total = sum(total_jurisdictions)
#   ) |>
#   ungroup() |>
#   mutate(
#     `Not Started` = Total - Published - `In Progress` - Claimed,
#     Total = ifelse(`Not Started` < 0, Total + abs(`Not Started`), Total),
#     `Not Started` = Total - Published - `In Progress` - Claimed
#   ) |>
#   select(CSAID, CSA, population, `Not Started`, Claimed:Total) |>
#   arrange(-population) |>
#   # fix Worcester, MA-CT
#   mutate(
#     Published = ifelse(str_detect(CSA, "CT"), Published + 16, Published),
#     Claimed = ifelse(str_detect(CSA, "CT"), Claimed - 16, Claimed)
#   ) |>
#   print()
# 
# 
# ## add in missing jurisdictions
# msa_juris_missing <- jurisdictions_by_county_state_msa |>
#   filter(!CSAID %in% msa_juris_prep$CSAID) |>
#   group_by(CSAID, CSA, population) |>
#   summarize(Total = sum(total_jurisdictions)) |>
#   ungroup() |>
#   mutate(
#     `Not Started` = Total,
#     Claimed = 0,
#     `In Progress` = 0,
#     Published = 0
#   ) |>
#   select(CSAID, CSA, population, `Not Started`:Published, Total) |>
#   arrange(-population) |>
#   # fix CT
#   mutate(
#     Published = ifelse(str_detect(CSA, "CT"), `Not Started`, Published),
#     `Not Started` = ifelse(str_detect(CSA, "CT"), 0, `Not Started`)
#   ) |>
#   print()
# 
# 
# ## combine
# msa_juris_report <- bind_rows(msa_juris_prep, msa_juris_missing) |>
#   arrange(-population) |>
#   mutate(Total = `Not Started` + Claimed + Published) |>
#   print()


## -------------------------------------------------------
## Save out
## -------------------------------------------------------

# date_time <- paste0(str_replace_all(str_sub(Sys.time(), 1, 10), "-", ""), "_", str_replace_all(str_sub(Sys.time(), 12, 19), ":", ""))
# filename <- paste0("reports/msa_progressreport", "_", date_time, ".csv")
# write_csv(msa_juris_report, filename)


## -------------------------------------------------------
## HUD report
## -------------------------------------------------------

hud_import <- read.xlsx("reports/hud_report.xlsx") |>
  as_tibble() |>
  select(CSAID, Funding.Stream) |>
  rename(
    `Funding Stream` = "Funding.Stream"
  ) |>
  mutate(CSAID = as.character(CSAID)) |>
  print()


hud_msa_report <- msa_juris_report |>
  filter(row_number() <= 40) |>
  select(
    -`In Progress`,
    `In Progress` = "Claimed"
  ) |>
  left_join(hud_import, by = "CSAID") |>
  print()


## -------------------------------------------------------
## Save out
## -------------------------------------------------------

date<- paste0(str_replace_all(str_sub(Sys.time(), 1, 7), "-", ""))
filename <- paste0("reports/hud_msa_report", "_", date, ".csv")
write_csv(hud_msa_report, filename)


## -------------------------------------------------------
## Update Lauerman file
## -------------------------------------------------------

## load lauerman file
lauerman_import <- read.xlsx("input_tables/lauerman_coastal_cbsas.xlsx", sheet = 1) |>
  as_tibble() |>
  print()

## add lauerman column
lauerman_fix <- lauerman_import |>
  mutate(GEOID = as.character(GEOID)) |>
  left_join(msa_juris_report[c("CSAID", "Not Started", "Claimed", "In Progress", "Published", "Total")], by = c("GEOID" = "CSAID")) |>
  rename(`Total Jurisdictions` = Total) |>
  print()

write.xlsx(lauerman_fix, "input_tables/lauerman_cbsas_update.xlsx", sheetName = "In Scope")
