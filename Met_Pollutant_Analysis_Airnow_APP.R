# =========================================================================
# Shiny App for Site-Specific Pollutant & Meteorology Analysis
# Version: 2024-04-09_Shiny_HourlyDaily
# Supports Hourly (AirNow Hourly + NOAA ISH) & Daily (AirNow Daily + IEM ASOS)
# =========================================================================

# --- 1. Load Libraries ---
# install.packages(c("shiny", "dplyr", "lubridate", "worldmet", "openair", "readr", "ggplot2", "gridExtra", "grid", "viridis", "padr", "httr", "purrr", "future", "furrr", "DT", "shinycssloaders", "shinyjs", "zip", "glue"))
library(shiny)
library(bslib)
library(dplyr)
library(digest)
library(lubridate)
library(worldmet) # For importNOAA (hourly met)
library(openair)
library(readr)
library(ggplot2)
library(gridExtra) # For grid.arrange
library(grid)      # For grid.arrange elements
library(viridis)
library(padr)      # For padding time series (hourly)
library(httr)
library(purrr)
library(future)
library(furrr)
library(DT)
library(shinycssloaders)
library(shinyjs)
library(zip)
library(glue)      # For string interpolation (IEM URL)
library(tidyr)
library(mgcv)
library(RColorBrewer)
library(geosphere)
library(quantreg)

# --- 2. Global Setup ---

# Constants
AIRNOW_HOURLY_BASE_URL <- "https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow"
AIRNOW_DAILY_BASE_URL <- "https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow" # Same base, different path structure
IEM_DAILY_MET_URL_TEMPLATE <- "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py?network={network}&stations={station_id}&year1={sy}&month1={sm}&day1={sd}&year2={ey}&month2={em}&day2={ed}&vars%5B%5D=max_tmpf&vars%5B%5D=min_tmpf&vars%5B%5D=max_dwpf&vars%5B%5D=min_dwpf&vars%5B%5D=pday&vars%5B%5D=avg_sknt&vars%5B%5D=avg_drct&vars%5B%5D=min_rh&vars%5B%5D=avg_rh&vars%5B%5D=max_rh&vars%5B%5D=snow&vars%5B%5D=snowd&vars%5B%5D=min_feel&vars%5B%5D=avg_feel&vars%5B%5D=max_feel&vars%5B%5D=max_sped&vars%5B%5D=max_gust&vars%5B%5D=srad_mj&format=csv"

# --- HOURLY BREAKS ---
O3_HOURLY_BREAKS_PPM <- c(0, 0.054001, 0.070001, 0.085001, 0.105001, Inf)
O3_HOURLY_LABELS <- c("Good", "Moderate", "USG", "Unhealthy", "Very Unhealthy")
O3_HOURLY_COLS <- c("green", "yellow", "orange", "red", "purple")

PM25_HOURLY_BREAKS_UGM3 <- c(0, 9.0, 35.4, 55.4, 150.4, 250.4, Inf) # Current EPA NowCast breaks approx
PM25_HOURLY_LABELS <- c("Good", "Moderate", "USG", "Unhealthy", "Very Unhealthy", "Hazardous")
PM25_HOURLY_COLS <- c("green", "yellow", "orange", "red", "purple", "maroon")

# --- DAILY BREAKS (Based on Standard AQI) ---
# Note: Ozone 8hr AQI is complex (averaging, truncation). These are breakpoints for direct comparison.
O3_DAILY_8HR_BREAKS_PPB <- c(0, 54, 70, 85, 105, 200, Inf) # PPB values corresponding to AQI 50, 100, 150, 200, 300
O3_DAILY_8HR_LABELS <- c("Good", "Moderate", "USG", "Unhealthy", "Very Unhealthy", "Hazardous")
O3_DAILY_8HR_COLS <- c("green", "yellow", "orange", "red", "purple", "maroon")

# PM2.5 Daily AQI breaks (24hr avg)
PM25_DAILY_24HR_BREAKS_UGM3 <- c(0, 9.0, 35.4, 55.4, 150.4, 250.4, Inf) # UGM3 values for AQI 50, 100, 150, 200, 300
PM25_DAILY_24HR_LABELS <- c("Good", "Moderate", "USG", "Unhealthy", "Very Unhealthy", "Hazardous")
PM25_DAILY_24HR_COLS <- c("green", "yellow", "orange", "red", "purple", "maroon")

# Plot Choices
plot_choices <- list(
  "Calendar Plots"="calendar",
  "Trend Plots (Smooth/CPF)"="trend", # CPF needs hourly
  "Theil-Sen Trend (Multi-Year)"="theilsen",
  "Time Series Plots"="timeseries",
  "ggplot Trend"="ggtrend",
  "Polar Plots"="polar", # Needs ws/wd
  "Pollution Roses"="polrose", # Needs ws/wd
  "Scatter Plots (Pollutant vs Met)"="scatter_met",
  "Scatter Plots (Pollutant vs Other)"="scatter_other", # Not applicable?
  "Correlation Plot"="corr",
  "TrendLevel Plots"="trendlevel", # Needs hourly
  "Time Variation Plots"="timevar", # Needs hourly
  "Polar Annulus (by Temp)"="polarannulus", # Needs hourly
  "Data Summary Plot"="summary",
  "Wind Rose / Linear Relation"="additional",
  "Polar Cluster Analysis"="polarcluster",
  "Percentile Rose (Multiple)"="percentilerose_multi",
  "Kernel Density Plot"="kernelexceed",
  "Conditional Polar Plot (by Temp)"="polarconditional",
  "Wind-Normalized Trend"="windnorm",
  "Stability Analysis (Hourly)"="stability"
)


# --- Download and Load AQS Sites Data (REVISED) ---
aqs_sites_zip_url <- "https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip"
local_zip_file <- "aqs_sites.zip"
extracted_csv_name <- "aqs_sites.csv"
sites_data_full <- NULL
states_data_distinct <- NULL

load_aqs_sites_data_refactored <- function() {
  data_dir <- "app_data"
  dir.create(data_dir, showWarnings = FALSE)
  local_zip_path <- file.path(data_dir, local_zip_file)
  local_csv_path <- file.path(data_dir, extracted_csv_name)
  
  if (!file.exists(local_csv_path)) {
    print(paste("Attempting download for AQS sites:", aqs_sites_zip_url))
    download_ok <- FALSE
    
    # --- MODIFICATION START: Use download.file with a much longer timeout ---
    tryCatch({
      # Increase timeout to 600 seconds (10 minutes) for this large file
      # Using method="curl" is generally more robust across platforms
      result <- download.file(aqs_sites_zip_url, destfile = local_zip_path, mode = "wb", quiet = FALSE, method = "curl", extra = "-L", timeout = 600)
      
      if (result == 0 && file.exists(local_zip_path)) {
        print("AQS Sites ZIP downloaded successfully.")
        download_ok <- TRUE
      } else {
        warning(paste("AQS Sites ZIP download failed. download.file status:", result), immediate. = TRUE)
      }
    }, error = function(e) {
      warning(paste("AQS Sites ZIP download error:", e$message), immediate. = TRUE)
    })
    # --- MODIFICATION END ---
    
    if (download_ok && file.exists(local_zip_path)) {
      print("Unzipping AQS sites data...")
      tryCatch({
        unzip(local_zip_path, files = extracted_csv_name, exdir = data_dir, overwrite = TRUE)
        print("AQS Sites data unzipped successfully.")
      }, error = function(e) {
        warning(paste("Failed to unzip AQS sites data:", e$message), immediate. = TRUE)
        if (file.exists(local_csv_path)) try(file.remove(local_csv_path), silent = TRUE)
        return(NULL)
      })
    } else if (!download_ok) {
      warning("AQS Sites ZIP download was not successful, cannot proceed with unzipping.", immediate. = TRUE)
      return(NULL)
    }
    
    if (file.exists(local_zip_path)) try(file.remove(local_zip_path), silent = TRUE)
    
  } else {
    print(paste("Using existing AQS sites CSV:", local_csv_path))
  }
  
  if (file.exists(local_csv_path)) {
    print("Reading and processing AQS Sites CSV...")
    tryCatch({
      sites_df_raw <- readr::read_csv(local_csv_path, show_col_types = FALSE, progress = FALSE, guess_max = 150000)
      
      req_cols <- c("State Code", "County Code", "Site Number", "Latitude", "Longitude", 
                    "GMT Offset", "State Name", "County Name", "Local Site Name")
      if (!all(req_cols %in% names(sites_df_raw))) {
        missing_actual <- setdiff(req_cols, names(sites_df_raw))
        stop(paste("Missing required columns in aqs_sites.csv. Expected:", paste(req_cols, collapse=", "), 
                   "\nMissing:", paste(missing_actual, collapse=", ")))
      }
      
      sites_df <- sites_df_raw %>%
        mutate(
          `State Code Num` = suppressWarnings(as.integer(`State Code`)),
          `County Code Num` = suppressWarnings(as.integer(`County Code`)),
          `Site Number Num` = suppressWarnings(as.integer(`Site Number`)),
          Latitude = suppressWarnings(as.numeric(Latitude)),
          Longitude = suppressWarnings(as.numeric(Longitude)),
          `GMT Offset Num` = suppressWarnings(as.numeric(`GMT Offset`))
        ) %>%
        mutate(
          `State Code Fmt` = if_else(is.na(`State Code Num`), NA_character_, sprintf("%02d", `State Code Num`)),
          `County Code Fmt` = if_else(is.na(`County Code Num`), NA_character_, sprintf("%03d", `County Code Num`)),
          `Site Number Fmt` = if_else(is.na(`Site Number Num`), NA_character_, sprintf("%04d", `Site Number Num`))
        ) %>%
        mutate(AQSID = paste0(`State Code Fmt`, `County Code Fmt`, `Site Number Fmt`)) %>%
        mutate(
          InferredTZ = case_when(
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -10 ~ "Pacific/Honolulu",
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -9  ~ "America/Anchorage",
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -8  ~ "America/Los_Angeles",
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -7  ~ "America/Denver",
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -6  ~ "America/Chicago",
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -5  ~ "America/New_York",
            !is.na(`GMT Offset Num`) & `GMT Offset Num` == -4  ~ "America/Puerto_Rico",
            TRUE ~ "UTC"
          )
        ) %>%
        filter(
          !is.na(AQSID) & nchar(AQSID) == 9,
          !is.na(Latitude) & !is.na(Longitude),
          (Latitude != 0 | Longitude != 0),
          !is.na(`State Name`) & `State Name` != "",
          !is.na(`County Name`) & `County Name` != "",
          !is.na(`Local Site Name`) & `Local Site Name` != ""
        ) %>%
        mutate(SiteNameID = paste(`Local Site Name`, "-", `Site Number Fmt`)) %>%
        select(
          `State Code` = `State Code Fmt`,
          `County Code` = `County Code Fmt`,
          `Site Number` = `Site Number Fmt`,
          AQSID, SiteNameID, Latitude, Longitude, 
          `GMT Offset` = `GMT Offset Num`,
          InferredTZ, 
          `State Name`, `County Name`, `Local Site Name`
        )
      
      print("AQS Sites processing completed successfully.")
      return(sites_df)
      
    }, error = function(e) {
      warning(paste("CRITICAL: Failed to process AQS Sites CSV '", extracted_csv_name, "'. Error: ", e$message), immediate. = TRUE)
      return(NULL)
    })
  } else {
    warning("CRITICAL: Extracted AQS Sites CSV '", extracted_csv_name, "' not found after download/unzip attempt.", immediate. = TRUE)
    return(NULL)
  }
}

# Replace the old call with the new function name
sites_data_full <- load_aqs_sites_data_refactored()
if(!is.null(sites_data_full) && nrow(sites_data_full) > 0) {
  states_data_distinct <- sites_data_full %>% 
    distinct(`State Code`, `State Name`) %>% 
    arrange(`State Name`)
  print(paste("Found", nrow(states_data_distinct), "distinct states from processed AQS sites data."))
} else {
  warning("AQS Site data is NULL or empty after processing. Site selection will be impaired.", immediate. = TRUE)
  states_data_distinct <- tibble(`State Code` = character(0), `State Name` = character(0))
}


# --- Helper Functions ---

# CACHING Helper for Pollutant Data
fetch_and_cache_pollutant_data <- function(site_info, selected_pollutant_code, start_d, end_d, data_type_mode, rv_log_update) {
  # Generate a unique key for this request using a hash
  cache_key <- digest::digest(list(site_info$aqs_id, selected_pollutant_code, start_d, end_d, data_type_mode))
  cache_dir <- "app_cache"
  dir.create(cache_dir, showWarnings = FALSE)
  cache_file <- file.path(cache_dir, paste0(cache_key, ".rds"))
  
  # Check for a valid cache file (less than 24 hours old)
  if (file.exists(cache_file)) {
    file_age_hours <- difftime(Sys.time(), file.mtime(cache_file), units = "hours")
    if (file_age_hours < 24) {
      rv_log_update(paste(" > Found valid cache for pollutant data (age:", round(file_age_hours, 1), "hrs). Loading from cache."))
      # Using tryCatch is robust in case the cache file is corrupted
      cached_data <- tryCatch({
        readRDS(cache_file)
      }, error = function(e) {
        rv_log_update(" > WARNING: Failed to read cache file, will re-fetch.")
        NULL
      })
      if (!is.null(cached_data)) return(cached_data)
    } else {
      rv_log_update(" > Pollutant cache file is stale (>24 hrs). Re-fetching data.")
    }
  }
  
  rv_log_update(" > No valid cache found. Fetching new pollutant data from source...")
  
  # --- Data Fetching Logic (moved from the reactive) ---
  data_r <- NULL
  if (data_type_mode == "hourly") {
    all_hours_gmt <- seq(from = as.POSIXct(paste(start_d, "00:00:00"), tz = "GMT"), to = as.POSIXct(paste(end_d, "23:00:00"), tz = "GMT"), by = "hour")
    all_urls <- sapply(all_hours_gmt, get_airnow_hourly_url, base_url = AIRNOW_HOURLY_BASE_URL)
    num_cores <- availableCores() - 1; if (num_cores < 1) num_cores <- 1
    plan(multisession, workers = num_cores)
    data_r <- furrr::future_map_dfr(all_urls, read_and_process_airnow_hourly,
                                    target_aqs_id = site_info$aqs_id, target_param = selected_pollutant_code,
                                    target_tz = site_info$tz, target_lat = site_info$lat, target_lon = site_info$lon,
                                    .progress = TRUE, .options = furrr_options(seed = TRUE, packages = c("readr", "dplyr", "lubridate")))
    plan(sequential)
  } else { # Daily
    all_dates <- seq.Date(from = start_d, to = end_d, by = "day")
    all_urls <- sapply(all_dates, get_airnow_daily_url, base_url = AIRNOW_DAILY_BASE_URL)
    data_r <- purrr::map_dfr(unique(all_urls), read_process_airnow_daily,
                             target_aqs_id = site_info$aqs_id, target_param_name = selected_pollutant_code,
                             target_lat = site_info$lat, target_lon = site_info$lon, .progress = TRUE)
    if (!is.null(data_r)) { data_r <- data_r %>% filter(date >= start_d, date <= end_d) }
  }
  
  # --- Save to cache if data was successfully fetched ---
  if (!is.null(data_r) && nrow(data_r) > 0) {
    tryCatch({
      saveRDS(data_r, cache_file)
      rv_log_update(" > Successfully saved new pollutant data to cache.")
    }, error = function(e) {
      rv_log_update(paste(" > WARNING: Failed to save pollutant data to cache:", e$message))
    })
  }
  
  return(data_r)
}

# CACHING Helper for MET Data
fetch_and_cache_met_data <- function(met_code, start_d, end_d, data_type_mode, site_info, plot_dir, met_state_abbr, rv_log_update) {
  # Generate a unique key for this request
  cache_key <- digest::digest(list(met_code, start_d, end_d, data_type_mode, met_state_abbr))
  cache_dir <- "app_cache"
  dir.create(cache_dir, showWarnings = FALSE)
  cache_file <- file.path(cache_dir, paste0(cache_key, ".rds"))
  
  # Check for a valid cache file
  if (file.exists(cache_file)) {
    file_age_hours <- difftime(Sys.time(), file.mtime(cache_file), units = "hours")
    if (file_age_hours < 24) {
      rv_log_update(paste(" > Found valid cache for MET data (age:", round(file_age_hours, 1), "hrs). Loading from cache."))
      cached_data <- tryCatch(readRDS(cache_file), error = function(e) {
        rv_log_update(" > WARNING: Failed to read MET cache file, will re-fetch.")
        NULL
      })
      if (!is.null(cached_data)) return(cached_data)
    } else {
      rv_log_update(" > MET cache file is stale (>24 hrs). Re-fetching data.")
    }
  }
  
  rv_log_update(" > No valid MET cache found. Fetching new MET data from source...")
  
  # --- MET Data Fetching Logic ---
  data_r <- NULL
  if (data_type_mode == "hourly") {
    met_data_list <- list()
    for (yr in year(start_d):year(end_d)) {
      current_year_met <- tryCatch({
        importNOAA(code = met_code, year = yr, quiet = TRUE, path = plot_dir, n.cores = 1)
      }, error = function(e) { NULL })
      if (!is.null(current_year_met)) met_data_list[[as.character(yr)]] <- current_year_met
    }
    if (length(met_data_list) > 0) {
      data_r <- bind_rows(met_data_list)
      # --- MODIFICATION: Rename columns to match openair standards ---
      if (!is.null(data_r) && nrow(data_r) > 0) {
        data_r <- data_r %>%
          rename_with(~"temp", any_of("air_temp")) %>%
          rename_with(~"rh", any_of("RH"))
      }
    }
  } else { # Daily
    data_r <- fetch_process_iem_daily_met(
      station_id = met_code, start_date = start_d, end_date = end_d,
      met_state_abbr = met_state_abbr, met_dir = plot_dir
    )
  }
  
  # --- Save to cache if data was successfully fetched ---
  if (!is.null(data_r) && nrow(data_r) > 0) {
    tryCatch({
      saveRDS(data_r, cache_file)
      rv_log_update(" > Successfully saved new MET data to cache.")
    }, error = function(e) {
      rv_log_update(paste(" > WARNING: Failed to save MET data to cache:", e$message))
    })
  }
  
  return(data_r)
}

# Fetch and cache the full NOAA ISH station metadata for the US
get_us_ish_metadata <- function(cache_dir = "app_data") {
  dir.create(cache_dir, showWarnings = FALSE)
  meta_file_path <- file.path(cache_dir, "us_ish_metadata.rds")
  
  # Use cache if it's less than 30 days old
  if (file.exists(meta_file_path)) {
    if (difftime(Sys.time(), file.mtime(meta_file_path), units = "days") < 30) {
      print("Loading US MET station metadata from cache.")
      return(readRDS(meta_file_path))
    }
  }
  
  print("Downloading US MET station metadata state by state... (this may take over a minute)")
  
  # List of US states and territories
  us_states_territories <- c("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
                             "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
                             "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
                             "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
                             "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
                             "AS", "GU", "MP", "PR", "VI")
  
  all_us_meta_list <- tryCatch({
    purrr::map(us_states_territories, function(st) {
      Sys.sleep(0.1)
      state_meta <- worldmet::getMeta(state = st)
      if (!is.null(state_meta) && nrow(state_meta) > 0) {
        state_meta <- state_meta %>% dplyr::mutate(state = st)
      }
      return(state_meta)
    }, .progress = TRUE)
  }, error = function(e) {
    warning("Could not download all MET station metadata from worldmet. Error: ", e$message, immediate. = TRUE)
    return(NULL)
  })
  
  if (!is.null(all_us_meta_list)) {
    all_us_meta <- bind_rows(all_us_meta_list)
    
    if (nrow(all_us_meta) > 0) {
      us_meta_clean <- all_us_meta %>%
        filter(!is.na(station), station != "", !is.na(state), nchar(state) == 2) %>%
        # *** THE CRITICAL FIX IS HERE ***
        # Standardize coordinate column names to 'lat' and 'lon'
        select(code, station, state, lat = latitude, lon = longitude) %>%
        mutate(search_name = paste0(station, " - ", code))
      
      saveRDS(us_meta_clean, meta_file_path)
      print("MET station metadata downloaded and cached successfully.")
      return(us_meta_clean)
    }
  }
  
  warning("Failed to download any MET station metadata from worldmet.", immediate. = TRUE)
  return(NULL)
}

# Pre-load the metadata when the app starts
us_ish_stations <- get_us_ish_metadata()

# Fetch and cache the full ASOS station metadata for the US from IEM
get_us_asos_metadata <- function(cache_dir = "app_data") {
  dir.create(cache_dir, showWarnings = FALSE)
  meta_file_path <- file.path(cache_dir, "us_asos_metadata.rds")
  
  if (file.exists(meta_file_path)) {
    if (difftime(Sys.time(), file.mtime(meta_file_path), units = "days") < 30) {
      print("Loading US ASOS station metadata from cache.")
      return(readRDS(meta_file_path))
    }
  }
  
  print("Downloading US ASOS/AWOS station metadata from IEM...")
  asos_url <- "https://mesonet.agron.iastate.edu/sites/networks.php?network=_ALL_&format=csv&nohtml=on"
  
  us_asos_meta <- tryCatch({
    readr::read_csv(asos_url, show_col_types = FALSE)
  }, error = function(e) {
    warning("Could not download ASOS station metadata from IEM.", immediate. = TRUE)
    return(NULL)
  })
  
  if (!is.null(us_asos_meta)) {
    # --- DEFINITIVE FIX BASED ON FILE SAMPLE ---
    us_asos_clean <- us_asos_meta %>%
      # The state is the first 2 characters of the 'iem_network' column
      mutate(state = substr(iem_network, 1, 2)) %>%
      # Filter for networks that are ASOS or AWOS for any US state
      filter(grepl("_ASOS|_AWOS", iem_network), nchar(state) == 2) %>%
      # Select and rename the correct columns
      select(station_id = stid, station_name, lat, lon, state) %>%
      # Ensure coordinates are valid
      filter(!is.na(lat), !is.na(lon))
    
    saveRDS(us_asos_clean, meta_file_path)
    print("ASOS station metadata downloaded and cached successfully.")
    return(us_asos_clean)
  }
  
  return(NULL)
}

# Pre-load both metadata sets when the app starts
us_ish_stations <- get_us_ish_metadata()
us_asos_stations <- get_us_asos_metadata()

# Find MET stations within a given radius of a point
find_nearby_stations <- function(aqi_lat, aqi_lon, met_stations_df, initial_radius_km = 25, max_radius_km = 100) {
  if (is.null(met_stations_df) || nrow(met_stations_df) == 0) return(NULL)
  
  # Calculate distance from AQS site to all MET stations
  distances_km <- distHaversine(
    p1 = c(aqi_lon, aqi_lat), 
    p2 = met_stations_df[, c("lon", "lat")]
  ) / 1000 # Convert meters to km
  
  met_stations_with_dist <- met_stations_df %>%
    mutate(distance_km = distances_km)
  
  # Find stations within the initial radius
  nearby <- met_stations_with_dist %>%
    filter(distance_km <= initial_radius_km) %>%
    arrange(distance_km)
  
  # If none found, expand the search radius
  if (nrow(nearby) == 0) {
    nearby <- met_stations_with_dist %>%
      filter(distance_km <= max_radius_km) %>%
      arrange(distance_km)
  }
  
  return(nearby)
}

# HOURLY AirNow Helpers
get_airnow_hourly_url<-function(timestamp_gmt,base_url){y<-format(timestamp_gmt,"%Y");d<-format(timestamp_gmt,"%Y%m%d");h<-format(timestamp_gmt,"%H");paste0(base_url,"/",y,"/",d,"/HourlyData_",d,h,".dat")}
read_and_process_airnow_hourly<-function(url,target_aqs_id,target_param,target_tz,target_lat,target_lon){col_names<-c("vd","vt","aqsid","site","gmt_off","param","units","val","src");col_types_spec<-cols(.default=col_character(),val=col_double());data<-tryCatch({read_delim(url,"|",col_names=col_names,col_types=col_types_spec,trim_ws=TRUE,locale=locale(encoding="UTF-8"),progress=FALSE,guess_max=5000)},warning=function(w){NULL},error=function(e){if(!grepl("404|403",e$message)){warning(paste("ERR reading URL:",url,e$message),call.=FALSE)};NULL});if(is.null(data)||nrow(data)==0){return(NULL)};filt_data<-data %>% filter(aqsid==target_aqs_id & param==target_param);if(nrow(filt_data)==0){return(NULL)};proc_data<-filt_data %>% mutate(dt_gmt=mdy_hm(paste(vd,vt),tz="UTC",quiet=TRUE),date=if_else(!is.na(dt_gmt),with_tz(dt_gmt,target_tz),NA_POSIXct_),PollutantValue=if(target_param=="OZONE") val/1000 else val,units_of_measure=if(target_param=="OZONE") "PPM" else units,detection_limit=NA_real_,method="AirNow Hourly",latitude=target_lat,longitude=target_lon) %>% select(date,!!sym(target_param):=PollutantValue,units_of_measure,detection_limit,method,latitude,longitude) %>% filter(!is.na(date));if(nrow(proc_data)==0){return(NULL)};return(proc_data)}

# DAILY AirNow Helpers
get_airnow_daily_url <- function(date_obj, base_url) {
  y <- format(date_obj, "%Y")
  d <- format(date_obj, "%Y%m%d")
  paste0(base_url, "/", y, "/", d, "/daily_data_v2.dat")
}

# DAILY AirNow Helpers (Revised Renaming/Selection Logic)
read_process_airnow_daily <- function(url, target_aqs_id, target_param_name, target_lat, target_lon) {
  # Define column names based on daily_data_v2.dat format
  col_names <- c("valid_date", "aqsid", "site_name", "parameter_name", "reporting_units",
                 "value", "averaging_period", "data_source", "aqi", "aqi_category",
                 "latitude_daily", "longitude_daily", "full_aqsid")
  col_types_spec <- cols(.default = col_character(), value = col_double(),
                         aqi = col_integer(), aqi_category = col_integer(),
                         latitude_daily = col_double(), longitude_daily = col_double())
  
  data <- tryCatch({
    read_delim(url, "|", col_names = col_names, col_types = col_types_spec,
               trim_ws = TRUE, locale = locale(encoding = "UTF-8"),
               progress = FALSE, guess_max = 10000)
  }, warning = function(w) { NULL }, error = function(e) {
    if (!grepl("404|403", e$message)) { warning(paste("ERR reading Daily URL:", url, e$message), call. = FALSE) }
    NULL
  })
  
  if (is.null(data) || nrow(data) == 0) { return(NULL) }
  
  filt_data <- data %>%
    filter(aqsid == target_aqs_id, parameter_name == target_param_name)
  
  if (nrow(filt_data) == 0) { return(NULL) }
  
  target_col_name <- case_when(
    target_param_name == "OZONE-8HR" ~ "OZONE_8HR_PPB",
    target_param_name == "PM2.5-24hr" ~ "PM25_24HR_UGM3",
    TRUE ~ NA_character_ 
  )
  
  if (is.na(target_col_name)) {
    warning(paste("Could not determine target column name for parameter:", target_param_name), call. = FALSE)
    return(NULL) 
  }
  
  proc_data <- filt_data %>%
    mutate(
      date = mdy(valid_date, quiet = TRUE), 
      units_of_measure = reporting_units,
      method = "AirNow Daily"
    ) %>%
    filter(!is.na(date)) 
  
  if (nrow(proc_data) == 0 || !("value" %in% names(proc_data))) {
    return(NULL)
  }
  
  names(proc_data)[names(proc_data) == "value"] <- target_col_name
  
  proc_data <- proc_data %>%
    select(
      date,
      all_of(target_col_name), 
      units_of_measure,
      method,
      latitude = latitude_daily,
      longitude = longitude_daily
    )
  
  if (nrow(proc_data) == 0) { return(NULL) }
  
  # Removed debug prints:
  # print(paste("--- DEBUG: read_process_airnow_daily returning for", target_param_name, "---"))
  # try(print(str(proc_data)), silent=TRUE)
  
  return(proc_data)
}

# DAILY IEM Met Helper (More Robust Header Check with read_csv(comment="#"))
fetch_process_iem_daily_met <- function(station_id, start_date, end_date, met_state_abbr, met_dir) {
  state_abbr <- toupper(trimws(met_state_abbr))
  if (!grepl("^[A-Z]{2}$", state_abbr)) {
    warning(paste("Invalid MET State Abbreviation provided:", met_state_abbr, ". Must be 2 letters. IEM URL may fail."), call. = FALSE, immediate. = TRUE)
  }
  network_code <- paste0(state_abbr, "_ASOS")
  sy <- year(start_date); sm <- month(start_date); sd <- day(start_date)
  ey <- year(end_date);   em <- month(end_date);   ed <- day(end_date)
  url <- glue::glue(IEM_DAILY_MET_URL_TEMPLATE, network = network_code, station_id = station_id, sy = sy, sm = sm, sd = sd, ey = ey, em = em, ed = ed)
  # Removed debug print for URL here, can be added back if needed
  # print(paste(" > IEM Met URL:", url)) 
  local_met_file <- file.path(met_dir, paste0("IEM_Daily_", station_id, "_", sy, sm, sd, "_", ey, em, ed, ".csv"))
  met_data <- NULL
  
  tryCatch({
    req_get <- httr::GET(url, httr::write_disk(local_met_file, overwrite = TRUE), httr::timeout(120))
    if (httr::status_code(req_get) == 200 && grepl("text", httr::http_type(req_get), ignore.case = TRUE) && file.exists(local_met_file)) {
      # print(paste(" > IEM Met data downloaded successfully to:", basename(local_met_file))) # Optional print
      
      # --- SIMPLIFIED HEADER READING using comment = "#" ---
      met_data_raw <- read_csv(local_met_file, comment = "#", na = "M", show_col_types = FALSE, lazy = FALSE)
      # --- End Simplified Header Reading ---
      
      # Debug prints for raw data can be useful during development, consider removing for production
      # print("--- DEBUG: Inside IEM Fetch - Raw Data Head ---")
      # try(print(head(met_data_raw)), silent = TRUE)
      # print("--- DEBUG: Inside IEM Fetch - Raw Data Structure ---")
      # try(print(str(met_data_raw)), silent = TRUE)
      
      required_cols_read <- c("day", "max_temp_f", "min_temp_f", "avg_wind_speed_kts", "avg_wind_drct", "avg_rh", "precip_in")
      # Optional cols are handled by any_of in select
      
      if (!is.null(met_data_raw) && nrow(met_data_raw) > 0 &&
          all(required_cols_read %in% names(met_data_raw))) {
        
        met_data <- met_data_raw %>%
          mutate(date = as.Date(day)) %>%
          # Ensure gust is numeric early; NA if conversion fails (e.g., "M" or non-numeric)
          mutate(max_wind_gust_kts = suppressWarnings(as.numeric(as.character(max_wind_gust_kts)))) %>% 
          mutate(temp_f_avg = rowMeans(select(., max_temp_f, min_temp_f), na.rm = TRUE),
                 temp = if_else(is.nan(temp_f_avg) | is.na(temp_f_avg), NA_real_, (temp_f_avg - 32) * 5/9)) %>%
          mutate(ws = avg_wind_speed_kts * 0.514444) %>% # kts to m/s
          select(
            date,
            temp, # Calculated C
            ws,   # Calculated m/s from kts
            wd = avg_wind_drct,
            rh = avg_rh,
            precip = precip_in, # IEM provides pday as precip_in
            # Select optional columns if they exist; any_of handles missing ones gracefully
            any_of(c(
              "max_temp_f", "min_temp_f", 
              "max_dewpoint_f", "min_dewpoint_f", 
              "min_rh", "max_rh", 
              "max_wind_speed_kts", "max_wind_gust_kts", # Already converted to numeric
              "srad_mj" # Solar radiation, if available and needed
            ))
          ) %>%
          filter(!is.na(date)) %>%
          arrange(date) %>%
          mutate(across(where(is.numeric), ~if_else(is.nan(.x), NA_real_, .x)))
        
        # Debug prints for processed data can be useful
        # print("--- DEBUG: Inside IEM Fetch - Processed Data Head ---")
        # try(print(head(met_data)), silent=TRUE)
        
      } else {
        # More informative message if critical columns are missing
        if (!is.null(met_data_raw)) {
          missing_req <- setdiff(required_cols_read, names(met_data_raw))
          if (length(missing_req) > 0) {
            warning(paste(" > IEM Met data read, but MISSING REQUIRED columns:", paste(missing_req, collapse=", ")), call. = FALSE)
          } else if (nrow(met_data_raw) == 0) {
            warning(" > IEM Met data read, but resulted in zero rows after initial read.", call. = FALSE)
          }
        } else {
          warning(" > IEM Met data (met_data_raw) is NULL after read attempt.", call. = FALSE)
        }
        met_data <- NULL # Ensure it's NULL if processing fails
      }
    } else {
      warning(paste(" > Failed to download or received non-text content for IEM Met data. Status:", 
                    httr::status_code(req_get), "Type:", httr::http_type(req_get)), call. = FALSE)
      met_data <- NULL
    }
  }, error = function(e) {
    warning(paste(" > Error fetching or processing IEM Met data:", e$message), call. = FALSE)
    met_data <- NULL
  })
  
  # Debug print before returning
  # print("--- DEBUG: Inside IEM Fetch - Returning Data Structure ---")
  # try(print(str(met_data)), silent=TRUE)
  return(met_data)
}

#' Generate Fine Breaks and Intra-Category Gradient Colors Based on AQI Categories (Revised)
#'
#' Creates finer breaks and a color vector where the gradient *within*
#' a category shifts towards the color of the *next* category. Avoids final unique() call.
#'
#' @param aqi_breaks Numeric vector of the main AQI threshold values (must include 0 and Inf).
#' @param aqi_colors Character vector of the main AQI colors (length must be length(aqi_breaks) - 1).
#' @param n_steps Integer, the number of color/break steps *within* each main AQI category. Must be >= 1.
#' @param upper_limit_mult Multiplier for the last finite break to define the upper bound when Inf is present.
#'
#' @return A list containing `breaks` (fine numeric vector) and `colors` (long character vector).
#'         Returns original breaks/colors if input validation fails or n_steps=0.
generate_aqi_intra_gradient_palette_revised <- function(aqi_breaks, aqi_colors, n_steps = 5, upper_limit_mult = 1.5) {
  
  # --- Input Validation ---
  if (!requireNamespace("grDevices", quietly = TRUE)) {
    stop("Package 'grDevices' needed for color ramps.")}
  if (length(aqi_colors) != length(aqi_breaks) - 1) {
    warning("Length mismatch: length(aqi_colors) must be length(aqi_breaks) - 1. Returning originals.", immediate. = TRUE)
    return(list(breaks = aqi_breaks, colors = aqi_colors))
  }
  if (!is.numeric(aqi_breaks) || !is.character(aqi_colors) || !is.numeric(n_steps) || n_steps < 1) {
    warning("Invalid input types or n_steps < 1. Returning originals.", immediate. = TRUE)
    return(list(breaks = aqi_breaks, colors = aqi_colors))
  }
  n_steps <- as.integer(n_steps)
  if (n_steps == 1) { # If only 1 step, just return originals (no gradient needed)
    return(list(breaks = aqi_breaks, colors = aqi_colors))
  }
  
  # --- Handle Inf break ---
  finite_breaks <- aqi_breaks
  inf_present <- is.infinite(finite_breaks[length(finite_breaks)])
  upper_limit <- NA_real_
  
  if (inf_present) {
    last_finite_idx <- length(finite_breaks) - 1
    if (last_finite_idx > 0) {
      # Ensure upper_limit is strictly greater than the last finite break
      upper_limit <- max(finite_breaks[last_finite_idx] * upper_limit_mult, finite_breaks[last_finite_idx] + 1e-6) # Add small epsilon
    } else { # Only had 0 and Inf
      upper_limit <- 100 # Arbitrary default if only 0 and Inf
    }
    finite_breaks[length(finite_breaks)] <- upper_limit
  } else {
    upper_limit <- finite_breaks[length(finite_breaks)] # Use the existing max if no Inf
  }
  # Ensure breaks are strictly increasing after potential Inf replacement
  finite_breaks <- sort(unique(finite_breaks)) # Use unique here ONLY to handle potential duplicates in input
  if(any(diff(finite_breaks) <= 0)) {
    warning("Breaks are not strictly increasing after initial processing. Returning originals.", immediate. = TRUE)
    return(list(breaks = aqi_breaks, colors = aqi_colors))
  }
  # Re-check color length against potentially modified finite_breaks length
  if (length(aqi_colors) != length(finite_breaks) - 1) {
    warning(paste("Color/finite_break length mismatch after initial processing.",
                  "Colors:", length(aqi_colors), "Intervals:", length(finite_breaks)-1,
                  "Returning originals."), immediate. = TRUE)
    return(list(breaks = aqi_breaks, colors = aqi_colors))
  }
  # --- End Inf Handling ---
  
  all_fine_breaks <- c()
  all_fine_colors <- c()
  
  # --- Loop through each main AQI category segment ---
  for (i in 1:length(aqi_colors)) {
    start_break <- finite_breaks[i]
    end_break <- finite_breaks[i+1]
    
    # Ensure start and end are different to avoid issues with seq()
    if (abs(start_break - end_break) < .Machine$double.eps^0.5) {
      if (length(all_fine_breaks) == 0 || all_fine_breaks[length(all_fine_breaks)] != start_break) {
        warning(paste("Zero-width interval found at index", i, "between", start_break, "and", end_break, ". Gradient generation might be compromised."), immediate. = TRUE)
        next # Skip to next iteration
      }
    }
    
    col_current <- aqi_colors[i]
    # Color toward which we shade (the next category's color)
    col_next <- if (i < length(aqi_colors)) aqi_colors[i + 1] else col_current # Last category shades to itself
    palette_func <- grDevices::colorRampPalette(c(col_current, col_next), space = "Lab")
    segment_colors <- palette_func(n_steps + 1)[1:n_steps]
    segment_break_points <- seq(start_break, end_break, length.out = n_steps + 1)
    all_fine_breaks <- c(all_fine_breaks, segment_break_points[1:n_steps])
    all_fine_colors <- c(all_fine_colors, segment_colors)
  }
  
  # Add the final absolute uppermost break value
  all_fine_breaks <- c(all_fine_breaks, upper_limit) # Add the last endpoint
  
  # --- Final Validation ---
  tolerance <- .Machine$double.eps^0.5
  keep_indices <- c(TRUE, diff(all_fine_breaks) > tolerance)
  all_fine_breaks <- all_fine_breaks[keep_indices]
  
  if (length(all_fine_breaks) != length(all_fine_colors) + 1) {
    warning(paste("Final length mismatch after generating fine breaks/colors.",
                  "Breaks:", length(all_fine_breaks), "Colors:", length(all_fine_colors),
                  "Likely due to precision issues or zero-width intervals.",
                  "Returning base AQI colors/breaks."), immediate. = TRUE)
    return(list(breaks = finite_breaks, colors = aqi_colors))
  }
  list(breaks = all_fine_breaks, colors = all_fine_colors)
}

# Calculate atmospheric stability class based on wind speed and time
calculate_stability <- function(df) {
  df %>%
    mutate(
      hour = hour(date),
      is_day = hour >= 6 & hour <= 18,
      stability = case_when(
        is_day & ws < 2 ~ "Very Unstable",
        is_day & ws >= 2 & ws < 3 ~ "Unstable",
        is_day & ws >= 3 & ws < 5 ~ "Slightly Unstable",
        is_day & ws >= 5 ~ "Neutral",
        !is_day & ws < 2 ~ "Very Stable",
        !is_day & ws >= 2 & ws < 3 ~ "Stable",
        !is_day & ws >= 3 & ws < 5 ~ "Slightly Stable",
        !is_day & ws >= 5 ~ "Neutral",
        TRUE ~ "Unknown"
      )
    )
}

# Data quality assessment function
assess_data_quality <- function(df, poll_col, met_cols = c("ws", "wd", "temp", "rh")) {
  # Ensure the dataframe is not empty
  if (is.null(df) || nrow(df) == 0) {
    return(list(Message = "Input data is empty. Cannot assess quality."))
  }
  
  quality_report <- list()
  
  # Overall pollutant completeness
  if (poll_col %in% names(df)) {
    quality_report$overall_completeness <- (nrow(df) - sum(is.na(df[[poll_col]]))) / nrow(df) * 100
  }
  
  # Temporal gaps (robustly handle single-row data)
  if ("date" %in% names(df) && nrow(df) > 1) {
    time_diffs <- diff(df$date)
    # Check if there are any valid time differences before calculating median
    if(any(!is.na(time_diffs))) {
      expected_interval <- median(time_diffs, na.rm = TRUE)
      # Check if expected interval is greater than zero to avoid infinite loops or errors
      if (expected_interval > 0) {
        quality_report$gaps <- sum(time_diffs > (expected_interval * 1.5), na.rm = TRUE) # Use 1.5* to be more sensitive
      } else {
        quality_report$gaps <- 0
      }
    } else {
      quality_report$gaps <- 0
    }
  } else {
    quality_report$gaps <- 0
  }
  
  # Met data completeness
  for (col in met_cols) {
    if (col %in% names(df)) {
      quality_report[[paste0(col, "_completeness")]] <- 
        (nrow(df) - sum(is.na(df[[col]]))) / nrow(df) * 100
    }
  }
  
  # Outlier detection (more robust against all-NA columns)
  if (poll_col %in% names(df) && sum(!is.na(df[[poll_col]])) > 0) {
    Q1 <- quantile(df[[poll_col]], 0.25, na.rm = TRUE)
    Q3 <- quantile(df[[poll_col]], 0.75, na.rm = TRUE)
    IQR_val <- Q3 - Q1
    # Check for zero IQR to avoid issues
    if (!is.na(IQR_val) && IQR_val > 0) {
      outliers <- sum(df[[poll_col]] < (Q1 - 3 * IQR_val) | 
                        df[[poll_col]] > (Q3 + 3 * IQR_val), na.rm = TRUE)
      quality_report$extreme_outliers <- outliers
    } else {
      quality_report$extreme_outliers <- 0
    }
  }
  
  return(quality_report)
}

# Plot Saving Helpers
save_and_print_plot <- function(plot_obj_func, filename, show_plot=TRUE, ...) { tryCatch({ png(filename,...); plot_obj_func(); dev.off() }, error=function(e){warning(paste("Fail save:",basename(filename)),call.=FALSE)}); if(show_plot){tryCatch({plot_obj_func()},error=function(e){})}; Sys.sleep(0.05) }
save_and_print_ggplot <- function(plot_obj, filename, show_plot=TRUE, ...) { tryCatch({ ggsave(filename, plot=plot_obj, ...) }, error=function(e){warning(paste("Fail save ggplot:",basename(filename)),call.=FALSE)}); if(show_plot){tryCatch({print(plot_obj)},error=function(e){})}; Sys.sleep(0.05) }
save_and_print_grid <- function(grid_obj, filename, show_plot=TRUE, ...) { tryCatch({ png(filename,...); grid.draw(grid_obj); dev.off() }, error=function(e){warning(paste("Fail save grid:",basename(filename)),call.=FALSE)}); if(show_plot){tryCatch({grid.draw(grid_obj)},error=function(e){})}; Sys.sleep(0.05) }


# =========================================================================
# --- UI Definition ---
# =========================================================================

ui <- page_sidebar(
  # --- JavaScript for custom messages ---
  tags$head(
    tags$script(HTML("
      Shiny.addCustomMessageHandler('scrollToBottom', function(message) {
        var element = document.getElementById(message.id);
        if(element) {
          element.scrollTop = element.scrollHeight;
        }
      });
      
      Shiny.addCustomMessageHandler('updateFeedback', function(message) {
        var element = document.getElementById(message.id);
        if(element) {
          element.textContent = message.text;
          element.className = message.class;
        }
      });
      
      $(document).keydown(function(e) {
        if((e.ctrlKey || e.metaKey) && e.keyCode == 13) {
          $('#run_button').click();
          e.preventDefault();
        }
        if((e.ctrlKey || e.metaKey) && e.keyCode == 76) {
          $('#clear_log').click();
          e.preventDefault();
        }
      });
    "))
  ),
  
  # --- Add a Modern Theme ---
  theme = bslib::bs_theme(
    version = 5,
    bootswatch = "flatly"
  ),
  
  # --- Include shinyjs ---
  useShinyjs(),
  
  # --- Title Panel ---
  titlePanel("AirNow Pollutant & Met Analysis"),
  
  # --- Sidebar Definition ---
  sidebar = sidebar(
    width = 300,
    h4("1. Analysis Type"),
    radioButtons("data_type", NULL,
                 choices = c("Hourly (AirNow Hourly + NOAA ISH)" = "hourly",
                             "Daily (AirNow Daily + IEM ASOS)" = "daily"),
                 selected = "hourly"),
    hr(),
    accordion(
      id = "sidebar_accordion",
      open = c("Pollutant & Site", "Date Range"),
      accordion_panel("2. Pollutant & Site", value = "Pollutant & Site", uiOutput("pollutant_selector_ui"),
                      selectInput("state_select", "Select State:", choices = if (!is.null(states_data_distinct) && nrow(states_data_distinct) > 0) setNames(states_data_distinct$`State Code`, states_data_distinct$`State Name`) else c("Site File Load Error" = ""), selected = "28"),
                      selectInput("county_select", "Select County:", choices = c("Select State" = "")),
                      selectInput("site_select", "Select AQS Site (Name-Num):", choices = c("Select County" = "")),
                      tags$small(textOutput("selected_site_details"))),
      accordion_panel("3. Meteorological Station", value = "Met Station", tags$div(id = "met_input_container", uiOutput("met_station_ui"))),
      accordion_panel("4. Date Range", value = "Date Range",
                      dateInput("start_date_input", "Start Date:", value = paste0(year(Sys.Date()) - 1, "-03-01"), format = "yyyy-mm-dd"),
                      dateInput("end_date_input", "End Date:", value = paste0(year(Sys.Date()) - 1, "-10-31"), format = "yyyy-mm-dd")),
      accordion_panel("5. Plots to Generate", value = "Plots",
                      checkboxGroupInput("plots_select", NULL, choices = plot_choices, selected = c("calendar", "ggtrend", "polar", "polrose", "scatter_met", "timevar", "additional")))
    ),
    hr(),
    
    # --- ADDED CONTACT INFO HERE ---
    tags$div(
      class = "text-center",
      style = "font-size: 0.9em; color: #7f8c8d; margin-bottom: 15px;",
      tags$p(
        "For bug reports or questions, please contact Rodney Cuevas at ",
        tags$a(href = "mailto:RCuevas@mdeq.ms.gov", "RCuevas@mdeq.ms.gov")
      )
    ),
    # --- END OF CONTACT INFO SECTION ---
    
    actionButton("run_button", "Run Analysis", icon = icon("play"), class = "btn-primary w-100"),
    actionButton("reset_button", "Reset Analysis", icon = icon("refresh"), class = "btn-warning w-100 mt-2"),
    hr(),
    h5("Status Log:"),
    tags$div(id = "status-box-container", style = "position: relative;",
             tags$div(id = "status-box", style = "height:200px; overflow-y:auto; border: 1px solid #dee2e6; border-radius: 4px; padding: 8px; background-color: #f8f9fa; font-size: 0.85em; line-height: 1.4;",
                      uiOutput("status_output_ui")),
             tags$div(style = "position: absolute; top: -25px; right: 0;",
                      actionButton("clear_log", "Clear", class = "btn-sm btn-outline-secondary", style = "padding: 2px 8px; font-size: 0.8em;"))
    )
  ), # End sidebar
  
  # --- Main Content Area ---
  navset_card_tab(
    id = "main_tabs",
    nav_panel("Merged Data", icon = icon("table"), card_body(fillable = FALSE, layout_sidebar(
      sidebar = sidebar(width = 200, position = "right", downloadButton("download_merged_data", "Download CSV", class = "btn-primary btn-sm w-100")),
      h4("Preview of Merged Data"), withSpinner(DT::dataTableOutput("merged_data_table"))))),
    nav_panel("Selected Plots", icon = icon("chart-line"), card_body(fillable = FALSE, layout_sidebar(
      sidebar = sidebar(width = 200, position = "right",
                        downloadButton("download_all_plots", "Download All Plots (ZIP)", class = "btn-primary btn-sm w-100 mb-2"),
                        selectInput("download_format", "Format:", choices = c("PNG" = "png", "PDF" = "pdf", "SVG" = "svg"), selected = "png")),
      h4("Generated Visualizations"), tags$p("Plots selected will appear below."), withSpinner(uiOutput("plots_dynamic_ui"))))),
    nav_panel("Statistics", icon = icon("calculator"), card_body(
      layout_columns(col_widths = c(3, 3, 3, 3), fill = FALSE,
                     uiOutput("stat_value_box_pollutant"), uiOutput("stat_value_box_exceedances"),
                     uiOutput("stat_value_box_ws"), uiOutput("stat_value_box_completeness")),
      tags$hr(),
      h4("Detailed Statistics"), tags$p("Statistics based on the merged data for the selected period."),
      withSpinner(uiOutput("statistics_dynamic_ui"))))
  )
  # NOTE: The footer argument has been removed from here.
  
) # End page_sidebar

# =========================================================================
# --- Server Logic ---
# =========================================================================
server <- function(input, output, session) {
  
  # --- Reactive Values ---
  rv <- reactiveValues(
    county_choices = NULL, site_choices = NULL, selected_site_info = NULL,
    pollutant_data = NULL, met_data = NULL, merged_data = NULL,
    status_log = c("App Initialized."), plot_dir = NULL,
    current_pollutant_code = NULL, # e.g., OZONE, OZONE-8HR
    current_pollutant_colname = NULL, # e.g., OZONE, OZONE_8HR_PPB
    current_datatype = "hourly", # Track current mode
    current_breaks = NULL, current_labels = NULL, current_colors = NULL,
    run_trigger = 0
  )
  
  # --- Initial UI State ---
  observe({
    # Check for MET station metadata first
    if (is.null(us_ish_stations)) {
      showNotification("Hourly MET station list failed to load. Manual entry may be required.", 
                       type = "warning", duration = 10)
    }
    
    # Then check for AQS site data
    if(is.null(sites_data_full)||is.null(states_data_distinct)||nrow(states_data_distinct)==0){
      disable("state_select");disable("county_select");disable("site_select")
      warning("AQS Site data failed to load or is empty. Site selection disabled.", immediate. = TRUE)
      rv$status_log <- tail(c(rv$status_log, "ERROR: AQS Site data failed to load. Cannot select site."), 20)
    } else {
      enable("state_select")
      # Try to populate county based on default state if available
      default_state <- isolate(input$state_select) # Use isolate to prevent dependency loops on startup
      if (!is.null(default_state) && nchar(default_state) > 0 && !is.null(sites_data_full)) {
        counties <- sites_data_full %>%
          filter(`State Code` == default_state) %>%
          distinct(`County Code`, `County Name`) %>%
          arrange(`County Name`)
        if (nrow(counties) > 0) {
          rv$county_choices <- setNames(counties$`County Code`, counties$`County Name`)
          updateSelectInput(session, "county_select", choices = c("Select County" = "", rv$county_choices), selected = "")
          enable("county_select")
        } else {
          disable("county_select")
          updateSelectInput(session, "county_select", choices = c("No Counties Found" = ""))
        }
      } else {
        disable("county_select")
        updateSelectInput(session, "county_select", choices = c("Select State First" = ""))
      }
      disable("site_select")
      updateSelectInput(session, "site_select", choices = c("Select County First" = ""))
    }
  })
  
  
  # --- Dynamic UI Updates ---
  
  # Update Pollutant Choices based on Data Type
  output$pollutant_selector_ui <- renderUI({
    req(input$data_type)
    rv$current_datatype <- input$data_type # Store current type
    
    if (input$data_type == "hourly") {
      selectInput("pollutant_select", "Select Pollutant:",
                  choices = c("Ozone (hourly, PPM)" = "OZONE", "PM2.5 (hourly, ug/m3)" = "PM2.5"),
                  selected = "OZONE")
    } else { # Daily
      selectInput("pollutant_select", "Select Pollutant Parameter:",
                  choices = c("Ozone 8hr Avg (PPB)" = "OZONE-8HR", "PM2.5 24hr Avg (ug/m3)" = "PM2.5-24hr"),
                  selected = "OZONE-8HR")
    }
  })
  
  # Update Met Station Input based on Data Type and Nearby Search
  output$met_station_ui <- renderUI({
    req(input$data_type)
    aqi_site_info <- rv$selected_site_info
    
    if (is.null(aqi_site_info)) {
      return(helpText("Select an AQS site to find nearby MET stations."))
    }
    
    nearby_stations <- NULL
    choices <- NULL
    
    if (input$data_type == "hourly") {
      nearby_stations <- find_nearby_stations(aqi_site_info$lat, aqi_site_info$lon, us_ish_stations)
      if (!is.null(nearby_stations) && nrow(nearby_stations) > 0) {
        # Create display names and values for the dropdown
        display_names <- sprintf("%s (%.1f km) - %s", nearby_stations$station, nearby_stations$distance_km, nearby_stations$code)
        station_values <- nearby_stations$code
        choices <- setNames(station_values, display_names)
      }
    } else { # Daily
      nearby_stations <- find_nearby_stations(aqi_site_info$lat, aqi_site_info$lon, us_asos_stations)
      if (!is.null(nearby_stations) && nrow(nearby_stations) > 0) {
        # CORRECTED: Use the 'station_name' column for the display name
        display_names <- sprintf("%s (%.1f km) - %s", nearby_stations$station_name, nearby_stations$distance_km, nearby_stations$station_id)
        station_values <- nearby_stations$station_id
        choices <- setNames(station_values, display_names)
      }
    }
    
    # If nearby stations were found, show a dropdown
    if (!is.null(choices)) {
      selectInput("met_station_select", "Select Nearby MET Station:",
                  choices = c("Select a station" = "", choices))
    } else {
      # If no stations were found, fall back to the original manual input UI
      if (input$data_type == "hourly") {
        tagList(
          helpText("No nearby NOAA ISH stations found within 100km. Please enter a code manually."),
          textInput("met_code_input", "Enter MET Station Code (NOAA ISH):", placeholder = "e.g., 723340-13876")
        )
      } else {
        tagList(
          helpText("No nearby ASOS stations found within 100km. Please enter an ID and State manually."),
          wellPanel(
            style = "background-color: #f8f9fa;",
            textInput("met_code_input", "1. Enter ASOS Station ID:", placeholder = "e.g., MEM"),
            textInput("met_state_input", "2. Enter Station's State:", placeholder = "e.g., TN")
          )
        )
      }
    }
  })
  
  
  # ADD THIS NEW OBSERVER BLOCK
  # Warn user about potential data lag for recent dates in hourly mode
  observe({
    req(input$end_date_input, input$data_type == "hourly")
    
    # Calculate the difference in days from today
    date_diff <- as.numeric(Sys.Date() - input$end_date_input)
    
    # If the end date is within the last 3 days, show a notification
    if (date_diff <= 2) {
      showNotification(
        "Note: Hourly MET data from NOAA may have a lag of 1-3 days. Data for very recent dates may be incomplete.",
        type = "warning",
        duration = 10 # Show the message for 10 seconds
      )
    }
  })
  
  # ADD THIS NEW OBSERVER:
  observe({
    met_code <- input$met_code_input
    if(is.null(met_code) || nchar(met_code) == 0) {
      session$sendCustomMessage("updateFeedback", 
                                list(id = "met_code_feedback", text = "", class = ""))
      return()
    }
    
    if(input$data_type == "hourly") {
      if(grepl("^[0-9]{5,6}-[0-9]{5}$", met_code) || grepl("^[A-Z]{4}$", met_code)) {
        session$sendCustomMessage("updateFeedback", 
                                  list(id = "met_code_feedback", 
                                       text = "✓ Valid format", 
                                       class = "text-success"))
      } else {
        session$sendCustomMessage("updateFeedback", 
                                  list(id = "met_code_feedback", 
                                       text = "✗ Invalid format", 
                                       class = "text-danger"))
      }
    }
  })
  
  
  # Observe State Select
  observeEvent(input$state_select,{
    req(input$state_select, sites_data_full)
    state_code_sel<-input$state_select
    counties<-sites_data_full %>%
      filter(`State Code`==state_code_sel) %>%
      distinct(`County Code`,`County Name`) %>%
      arrange(`County Name`)
    
    if(nrow(counties)>0){
      rv$county_choices<-setNames(counties$`County Code`,counties$`County Name`)
      updateSelectInput(session,"county_select",choices=c("Select County"="",rv$county_choices),selected="")
      enable("county_select")
    } else{
      rv$county_choices<-c("No Counties Found"="")
      updateSelectInput(session,"county_select",choices=rv$county_choices)
      disable("county_select")
    }
    # Reset downstream selections
    updateSelectInput(session,"site_select",choices=c("Select County First"=""),selected="")
    disable("site_select")
    rv$selected_site_info<-NULL # Reset site info
  },ignoreNULL=TRUE, ignoreInit = TRUE) # ignoreInit helps prevent initial double trigger
  
  # Observe County Select
  observeEvent(input$county_select,{
    req(input$state_select, input$county_select, nchar(input$county_select)>0, sites_data_full)
    state_code_sel<-input$state_select
    county_code_sel<-input$county_select
    sites_in_county<-sites_data_full %>%
      filter(`State Code`==state_code_sel & `County Code`==county_code_sel) %>%
      arrange(SiteNameID) # SiteNameID was created in load_aqs_sites_data
    
    if(nrow(sites_in_county)>0){
      rv$site_choices<-setNames(sites_in_county$AQSID,sites_in_county$SiteNameID)
      updateSelectInput(session,"site_select",choices=c("Select Site"="",rv$site_choices),selected="")
      enable("site_select")
    } else{
      rv$site_choices<-c("No sites found"="")
      updateSelectInput(session,"site_select",choices=rv$site_choices)
      disable("site_select")
    }
    rv$selected_site_info<-NULL # Reset site info
  },ignoreNULL=TRUE, ignoreInit = TRUE)
  
  # Observe Site Select - Get Details AND Find Nearby MET Stations
  observeEvent(input$site_select, {
    req(input$site_select, nchar(input$site_select) > 0, sites_data_full)
    selected_aqsid <- input$site_select
    site_details_df <- sites_data_full %>% filter(AQSID == selected_aqsid) %>% slice(1)
    
    if (nrow(site_details_df) > 0) {
      rv$selected_site_info <- list(
        aqs_id = site_details_df$AQSID,
        lat = site_details_df$Latitude,
        lon = site_details_df$Longitude,
        tz = site_details_df$InferredTZ,
        state_code = site_details_df$`State Code`,
        name_short = gsub("[^A-Za-z0-9_]", "", site_details_df$`Local Site Name`),
        name_long = paste0(site_details_df$`Local Site Name`, ", ", site_details_df$`County Name`, ", ", site_details_df$`State Name`)
      )
      rv$status_log <- tail(c(rv$status_log, paste("Site details loaded:", rv$selected_site_info$name_long)), 20)
      
      # NOTE: The nearby station search is triggered automatically by the UI renderer
      # which observes rv$selected_site_info. No explicit trigger is needed here.
      
    } else {
      rv$selected_site_info <- NULL;
      rv$status_log <- tail(c(rv$status_log, "ERROR: Site details not found for selected AQSID."), 20)
    }
  }, ignoreNULL = TRUE, ignoreInit = TRUE)
  
  # Display selected site details
  output$selected_site_details<-renderText({
    info<-rv$selected_site_info;
    if(!is.null(info)){
      paste("ID:",info$aqs_id,"| Lat:",round(info$lat,3),"| Lon:",round(info$lon,3),"| Inf. TZ:",info$tz)
    } else {
      "Awaiting site selection..."
    }
  })
  
  # Observe MET State Select for hourly data
  observeEvent(input$met_state_select, {
    req(input$met_state_select, nchar(input$met_state_select) > 0, us_ish_stations)
    
    # Filter stations based on the selected state
    # REVERTED: Back to filtering by 'state'
    stations_in_state <- us_ish_stations %>%
      filter(state == input$met_state_select) %>%
      arrange(station)
    
    if (nrow(stations_in_state) > 0) {
      # Create a named list for the choices: "Display Name" = "Value"
      station_choices <- setNames(stations_in_state$code, stations_in_state$search_name)
      updateSelectInput(session, "met_station_select",
                        choices = c("Select Station" = "", station_choices),
                        selected = "")
    } else {
      updateSelectInput(session, "met_station_select", choices = c("No stations found" = ""))
    }
  }, ignoreNULL = TRUE, ignoreInit = TRUE)
  
  # --- Main Analysis Trigger (MODIFY Daily Met Fetch Call) ---
  # --- Server Logic ---
  # (Inside server function)
  
  # --- A. Reactive Data Fetching & Merging ---
  
  # A-1. Pollutant Data Reactive (triggered by rv$run_trigger)
  pollutant_data <- eventReactive(rv$run_trigger, {
    req(rv$run_trigger > 0)
    site_info <- isolate(rv$selected_site_info)
    data_type_mode <- isolate(input$data_type)
    start_d <- isolate(input$start_date_input)
    end_d <- isolate(input$end_date_input)
    selected_pollutant_code <- isolate(input$pollutant_select)
    
    rv$status_log <- c(paste("Run:", format(Sys.time(), "%Y-%m-%d %H:%M"), "-", site_info$name_short, "-", selected_pollutant_code, paste0("(", data_type_mode, ")")),
                       paste("Site:", site_info$name_long, "(", site_info$aqs_id, ")"),
                       paste("Pollutant:", selected_pollutant_code),
                       paste("Dates:", start_d, "to", end_d),
                       "1. Fetching Pollutant Data...")
    
    data_r <- fetch_and_cache_pollutant_data(
      site_info = site_info, selected_pollutant_code = selected_pollutant_code,
      start_d = start_d, end_d = end_d, data_type_mode = data_type_mode,
      rv_log_update = function(msg) { rv$status_log <- tail(c(rv$status_log, msg), 25) }
    )
    return(data_r)
  })
  
  # A-2. Meteorological Data Reactive (triggered by rv$run_trigger)
  met_data <- eventReactive(rv$run_trigger, {
    req(rv$run_trigger > 0, pollutant_data())
    site_info <- isolate(rv$selected_site_info)
    data_type_mode <- isolate(input$data_type)
    start_d <- isolate(input$start_date_input)
    end_d <- isolate(input$end_date_input)
    met_code <- if (!is.null(input$met_station_select) && nchar(input$met_station_select) > 0) {
      input$met_station_select
    } else {
      trimws(isolate(input$met_code_input))
    }
    
    met_state <- if (data_type_mode == "daily") {
      # Try to find the state from the metadata if a station is selected from the dropdown
      station_info <- us_asos_stations %>% filter(station_id == met_code)
      if (nrow(station_info) > 0) {
        station_info$state[1] 
      } else {
        # Fallback to manual input if not found in dropdown
        trimws(isolate(input$met_state_input))
      }
    } else {
      NULL
    }
    
    data_r <- fetch_and_cache_met_data(
      met_code = met_code, start_d = start_d, end_d = end_d, data_type_mode = data_type_mode,
      site_info = site_info, plot_dir = isolate(rv$plot_dir), met_state_abbr = met_state,
      rv_log_update = function(msg) { rv$status_log <- tail(c(rv$status_log, msg), 25) }
    )
    return(data_r)
  })
  
  # A-3. Merged Data Reactive (triggered by rv$run_trigger)
  merged_data <- eventReactive(rv$run_trigger, {
    req(rv$run_trigger > 0)
    poll_data <- pollutant_data()
    met_data <- met_data()
    
    if (is.null(poll_data) || is.null(met_data)) {
      rv$status_log <- tail(c(rv$status_log, " > INFO: Merge skipped (pollutant or met data missing)."), 20)
      return(NULL)
    }
    
    rv$status_log <- tail(c(rv$status_log, "3. Merging Data..."), 20)
    merged <- tryCatch(left_join(poll_data, met_data, by = "date"), error = function(e) NULL)
    
    if (!is.null(merged) && nrow(merged) > 0) {
      rv$status_log <- tail(c(rv$status_log, paste(" > SUCCESS: Merged", nrow(merged), "rows.")), 20)
      return(merged)
    } else {
      rv$status_log <- tail(c(rv$status_log, " > WARNING: Merge resulted in 0 rows or failed."), 20)
      return(NULL)
    }
  })
  
  # A-4. UNIFIED OBSERVER for Run Button
  # This single block handles validation, data fetching, merging, and results.
  # This procedural approach guarantees sequential execution.
  observeEvent(input$run_button, {
    # --- Disable button at the start ---
    shinyjs::disable("run_button")
    
    # Use a tryCatch block with a finally statement to ensure the button is ALWAYS re-enabled
    tryCatch({
      
      # --- 1. COMPREHENSIVE INPUT VALIDATION ---
      site_info <- rv$selected_site_info
      if (is.null(site_info)) {
        showNotification("Please select an AQS site first.", type = "error"); return()
      }
      start_d <- input$start_date_input; end_d <- input$end_date_input
      if (is.na(start_d) || is.na(end_d) || end_d < start_d) {
        showNotification("Invalid date range selected.", type = "error"); return()
      }
      data_type_mode <- isolate(input$data_type)
      
      # --- REVISED MET CODE LOGIC ---
      met_code <- NULL
      met_state <- NULL # Only used for daily manual input
      
      if (data_type_mode == "hourly") {
        if (!is.null(input$met_station_select) && nchar(input$met_station_select) > 0) {
          met_code <- input$met_station_select
        } else if (!is.null(input$met_code_input)) {
          met_code <- trimws(input$met_code_input)
        }
      } else { # Daily
        if (!is.null(input$met_station_select) && nchar(input$met_station_select) > 0) {
          met_code <- input$met_station_select
          station_info <- us_asos_stations %>% filter(station_id == met_code)
          if (nrow(station_info) > 0) {
            met_state <- station_info$state[1]
          }
        } else if (!is.null(input$met_code_input)) {
          met_code <- trimws(input$met_code_input)
          met_state <- trimws(input$met_state_input)
        }
      }
      
      if (is.null(met_code) || nchar(met_code) < 3) {
        showNotification("Please select or enter a valid MET station code.", type = "error"); return()
      }
      if (data_type_mode == "daily" && (is.null(met_state) || nchar(met_state) != 2)) {
        showNotification("Could not determine the 2-letter state for the daily MET station.", type = "error"); return()
      }
      # --- END OF REVISED MET LOGIC ---
      
      # --- 2. SETUP ANALYSIS ENVIRONMENT ---
      rv$merged_data <- NULL 
      run_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      dir_analysis_out <- paste0(site_info$name_short, "_", data_type_mode, "_Analysis_", run_timestamp)
      dir.create(dir_analysis_out, showWarnings = FALSE)
      rv$plot_dir <- dir_analysis_out
      selected_pollutant_code <- isolate(input$pollutant_select)
      rv$current_pollutant_code <- selected_pollutant_code
      
      if (data_type_mode == "hourly") {
        rv$current_pollutant_colname <- selected_pollutant_code
        if (selected_pollutant_code == "OZONE") {
          rv$current_breaks <- O3_HOURLY_BREAKS_PPM; rv$current_labels <- O3_HOURLY_LABELS; rv$current_colors <- O3_HOURLY_COLS
        } else if (selected_pollutant_code == "PM2.5") {
          rv$current_breaks <- PM25_HOURLY_BREAKS_UGM3; rv$current_labels <- PM25_HOURLY_LABELS; rv$current_colors <- PM25_HOURLY_COLS
        }
      } else {
        if (selected_pollutant_code == "OZONE-8HR") {
          rv$current_pollutant_colname <- "OZONE_8HR_PPB"
          rv$current_breaks <- O3_DAILY_8HR_BREAKS_PPB; rv$current_labels <- O3_DAILY_8HR_LABELS; rv$current_colors <- O3_DAILY_8HR_COLS
        } else if (selected_pollutant_code == "PM2.5-24hr") {
          rv$current_pollutant_colname <- "PM25_24HR_UGM3"
          rv$current_breaks <- PM25_DAILY_24HR_BREAKS_UGM3; rv$current_labels <- PM25_DAILY_24HR_LABELS; rv$current_colors <- PM25_DAILY_24HR_COLS
        }
      }
      
      # --- 3. FETCH POLLUTANT DATA ---
      poll_data_result <- NULL
      withProgress(message = paste("Fetching", selected_pollutant_code), value = 0.2, {
        poll_data_result <- fetch_and_cache_pollutant_data(
          site_info = site_info, selected_pollutant_code = selected_pollutant_code,
          start_d = start_d, end_d = end_d, data_type_mode = data_type_mode,
          rv_log_update = function(msg) { rv$status_log <- tail(c(rv$status_log, msg), 25) }
        )
      })
      if (is.null(poll_data_result) || nrow(poll_data_result) == 0) {
        showNotification(paste("No", selected_pollutant_code, "data found."), type="warning"); return()
      }
      
      # --- 4. FETCH MET DATA ---
      met_data_result <- NULL
      withProgress(message = paste("Fetching MET data for", met_code), value = 0.5, {
        met_data_result <- fetch_and_cache_met_data(
          met_code = met_code, start_d = start_d, end_d = end_d, data_type_mode = data_type_mode,
          site_info = site_info, plot_dir = rv$plot_dir, met_state_abbr = met_state,
          rv_log_update = function(msg) { rv$status_log <- tail(c(rv$status_log, msg), 25) }
        )
      })
      if (is.null(met_data_result) || nrow(met_data_result) == 0) {
        showNotification(paste("No MET data found for station", met_code), type="warning"); return()
      }
      
      # --- 5. MERGE DATA ---
      rv$status_log <- tail(c(rv$status_log, "3. Merging Data..."), 20)
      merged_data_result <- tryCatch(left_join(poll_data_result, met_data_result, by = "date"), error = function(e) NULL)
      if (is.null(merged_data_result) || nrow(merged_data_result) == 0) {
        rv$status_log <- tail(c(rv$status_log, " > WARNING: Merge failed."), 20)
        showNotification("Data merging failed.", type="error"); return()
      }
      rv$status_log <- tail(c(rv$status_log, paste(" > SUCCESS: Merged data:", nrow(merged_data_result), "rows.")), 20)
      
      # --- 6. FINAL ASSIGNMENT & COMPLETION ---
      rv$merged_data <- merged_data_result
      rv$status_log <- tail(c(rv$status_log, "--- Analysis Complete ---"), 20)
      
    }, error = function(e) {
      rv$status_log <- tail(c(rv$status_log, "--- ANALYSIS FAILED ---", e$message), 25)
      showNotification(paste("An unexpected error occurred:", e$message), type = "error", duration = 10)
    }, finally = {
      shinyjs::enable("run_button")
    })
  })
  
  # A-5. OBSERVER for FINAL RESULTS
  observeEvent(merged_data(), {
    req(rv$run_trigger > 0)
    
    final_data <- merged_data()
    rv$merged_data <- final_data
    
    if (!is.null(final_data)) {
      rv$status_log <- tail(c(rv$status_log, "--- Analysis Complete ---"), 20)
    } else {
      rv$status_log <- tail(c(rv$status_log, "--- Analysis Incomplete ---"), 20)
    }
    
    shinyjs::enable("run_button")
    
  }, ignoreNULL = FALSE, ignoreInit = TRUE)
  
  # ADD THIS NEW OBSERVER:
  observeEvent(input$reset_button, {
    rv$pollutant_data <- NULL
    rv$met_data <- NULL
    rv$merged_data <- NULL
    rv$status_log <- c("Analysis reset.")
    rv$plot_dir <- NULL
    
    updateCheckboxGroupInput(session, "plots_select", 
                             selected = c("calendar", "ggtrend", "polar", "polrose", "scatter_met", "timevar", "additional"))
    
    showNotification("Analysis reset. Ready for new run.", type = "message")  # CHANGED FROM "info" TO "message"
  })
  
  # --- shinyjs setup for enabling/disabling plot choices ---
  # Define which plot *values* are hourly-only or daily-only
  # Values MUST match the 'plot_choices' list values
  hourly_only_plot_values <- c("trend", "trendlevel", "timevar", "polarannulus", "stability") # Added 'stability' # CPF is part of 'trend', TrendLevel needs hourly structure, TimeVar needs hourly
  daily_only_plot_values <- c() # Add any plots specifically for daily data if they arise
  
  observeEvent(input$data_type, {
    req(input$data_type)
    
    if (input$data_type == "hourly") {
      # Enable all potentially relevant plots for hourly
      # Disable any specifically daily plots (if any exist)
      for (plot_val in names(plot_choices)) {
        selector_id <- paste0("#plots_select input[value='", plot_choices[[plot_val]], "']")
        if(plot_choices[[plot_val]] %in% daily_only_plot_values) {
          shinyjs::disable(selector = selector_id)
        } else {
          shinyjs::enable(selector = selector_id)
        }
      }
      # Optional: Re-select a default set for hourly if desired
      # updateCheckboxGroupInput(session, "plots_select", selected = c("calendar", "trend", ...)) 
      
    } else { # Daily selected
      # Enable all potentially relevant plots for daily
      # Disable hourly-only plots
      for (plot_val in names(plot_choices)) {
        selector_id <- paste0("#plots_select input[value='", plot_choices[[plot_val]], "']")
        if(plot_choices[[plot_val]] %in% hourly_only_plot_values) {
          shinyjs::disable(selector = selector_id)
        } else {
          shinyjs::enable(selector = selector_id)
        }
      }
      # Optional: De-select disabled options if they were previously selected
      current_selection <- isolate(input$plots_select)
      valid_daily_selection <- current_selection[!current_selection %in% hourly_only_plot_values]
      updateCheckboxGroupInput(session, "plots_select", selected = valid_daily_selection)
    }
  }, ignoreNULL = TRUE) # ignoreNULL is important
  
  # --- Render Outputs ---
  output$status_output <- renderText({ paste(tail(rv$status_log, 20), collapse="\n") })
  
  # In the "Render Outputs" section
  output$merged_data_table <- DT::renderDataTable({
    # Use the reactive VALUE `rv$merged_data`
    validate(need(!is.null(rv$merged_data) && nrow(rv$merged_data) > 0, "No merged data available. Run analysis or check status log."))
    display_data <- rv$merged_data %>% mutate(across(where(is.numeric), ~round(.x, 3)))
    DT::datatable(display_data,
                  options = list(pageLength = 10, scrollX = TRUE, searching = FALSE),
                  rownames = FALSE)
  })
  
  # Replace the existing output$status_output
  output$status_output_ui <- renderUI({
    logs <- rv$status_log
    if(length(logs) == 0) {
      return(tags$p(style = "color: #6c757d; font-style: italic;", "No messages yet..."))
    }
    
    # Create styled log entries with timestamps and color coding
    log_entries <- lapply(seq_along(logs), function(i) {
      log_text <- logs[i]
      
      # Determine log type and color
      if(grepl("^ERR|ERROR:", log_text)) {
        color <- "#dc3545"  # Red for errors
        icon <- icon("exclamation-circle")
      } else if(grepl("^WARN|WARNING:", log_text)) {
        color <- "#ffc107"  # Yellow for warnings
        icon <- icon("exclamation-triangle")
      } else if(grepl("SUCCESS:", log_text)) {
        color <- "#28a745"  # Green for success
        icon <- icon("check-circle")
      } else if(grepl("^Run:", log_text)) {
        color <- "#007bff"  # Blue for run start
        icon <- icon("play-circle")
      } else {
        color <- "#495057"  # Default gray
        icon <- icon("info-circle")
      }
      
      tags$div(
        style = paste0("margin-bottom: 4px; color: ", color, ";"),
        icon,
        tags$span(style = "margin-left: 5px;", log_text)
      )
    })
    
    tagList(log_entries)
  })
  
  # --- Render Value Boxes for Statistics Tab ---
  # --- Render Value Boxes for Statistics Tab ---
  output$stat_value_box_pollutant <- renderUI({
    stats <- stats_reactive()
    req(stats$desc_stats_table)
    poll_col <- rv$current_pollutant_colname
    
    max_val <- stats$desc_stats_table %>% filter(Statistic == "Max") %>% pull(all_of(poll_col))
    mean_val <- stats$desc_stats_table %>% filter(Statistic == "Mean") %>% pull(all_of(poll_col))
    
    # --- DYNAMIC ROUNDING LOGIC ---
    # Determine rounding digits based on pollutant and its typical units/range
    display_digits_max <- case_when(
      grepl("OZONE", poll_col) && !grepl("PPB", poll_col) ~ 3, # Ozone in PPM needs 3 decimals
      grepl("PPB", poll_col) ~ 0,                              # Ozone in PPB is a whole number
      grepl("PM25|PM2.5", poll_col) ~ 1,                       # PM2.5 in ug/m3 needs 1 decimal
      TRUE ~ 2                                                # A sensible default
    )
    
    display_digits_mean <- case_when(
      grepl("OZONE", poll_col) && !grepl("PPB", poll_col) ~ 3, # Mean Ozone in PPM
      grepl("PPB", poll_col) ~ 1,                              # Mean PPB can have a decimal
      grepl("PM25|PM2.5", poll_col) ~ 2,                       # Mean PM2.5 can have more precision
      TRUE ~ 2                                                # A sensible default
    )
    
    bslib::value_box(
      title = paste("Max", poll_col),
      value = format(round(max_val, display_digits_max), nsmall = display_digits_max),
      showcase = icon("arrow-trend-up"),
      theme = "primary",
      p(paste("Mean:", format(round(mean_val, display_digits_mean), nsmall = display_digits_mean)))
    )
  })
  
  output$stat_value_box_exceedances <- renderUI({
    stats <- stats_reactive()
    req(stats$aqi_summary_table)
    
    unhealthy_cats <- c("USG", "Unhealthy", "Very Unhealthy", "Hazardous")
    exceed_count <- stats$aqi_summary_table %>%
      filter(Category %in% unhealthy_cats) %>%
      summarise(total = sum(Count, na.rm = TRUE)) %>%
      pull(total)
    
    bslib::value_box(
      title = "Days/Hours >= USG",
      value = exceed_count,
      showcase = icon("triangle-exclamation"),
      theme = if(exceed_count > 0) "danger" else "success",
      p("Based on AQI categories")
    )
  })
  
  output$stat_value_box_ws <- renderUI({
    stats <- stats_reactive()
    # Now requires the direct numeric value, which might be NULL if ws column is missing
    mean_ws <- stats$mean_scalar_ws
    
    bslib::value_box(
      title = "Mean Wind Speed",
      value = if (!is.null(mean_ws) && is.numeric(mean_ws)) paste(round(mean_ws, 2), "m/s") else "N/A",
      showcase = icon("wind"),
      theme = "info",
      p(if(is.null(mean_ws)) "Met data missing 'ws'" else "Scalar average")
    )
  })
  
  output$stat_value_box_completeness <- renderUI({
    stats <- stats_reactive()
    # Use the direct numeric value instead of parsing text
    completeness_pct <- stats$pollutant_completeness_percent
    
    bslib::value_box(
      title = "Pollutant Data Capture",
      value = if (!is.null(completeness_pct)) sprintf("%.1f%%", completeness_pct) else "N/A",
      showcase = icon("check-double"),
      theme = "secondary",
      p("Relative to merged data rows")
    )
  })
  
  # Add observer for clear log button
  observeEvent(input$clear_log, {
    rv$status_log <- c("Log cleared.")
  })
  
  # Add auto-scroll to bottom when new logs are added
  observe({
    rv$status_log
    session$sendCustomMessage(type = "scrollToBottom", message = list(id = "status-box"))
  })
  
  # --- Dynamic Plot UI ---
  # --- Dynamic Plot UI using bslib::card for Grouping ---
  output$plots_dynamic_ui <- renderUI({
    # --- Validation and Setup ---
    req(rv$merged_data) # Need merged data first
    poll_col <- rv$current_pollutant_colname
    req(poll_col) # Need the pollutant column name determined
    
    # Validate data presence and content
    validate(
      need(nrow(rv$merged_data) > 0, "Plotting skipped: Merged data is empty."),
      need(poll_col %in% names(rv$merged_data), paste("Plotting skipped: Column", poll_col, "not found.")),
      need(sum(!is.na(rv$merged_data[[poll_col]])) > 0, paste("Plotting skipped: No valid", poll_col, "data."))
    )
    
    selected_plots <- input$plots_select
    req(length(selected_plots) > 0) # Need at least one plot selected
    
    current_mode <- rv$current_datatype
    available_plots <- plot_choices # Use the global list
    
    # Define hourly-only plots (values from plot_choices)
    hourly_only_plot_values <- c("trend", "trendlevel", "timevar") # Check these match your actual hourly needs
    
    # Filter selected plots based on data type BEFORE generating UI
    if (current_mode == "daily") {
      selected_plots <- selected_plots[!selected_plots %in% hourly_only_plot_values]
    }
    
    # If no valid plots remain after filtering
    validate(need(length(selected_plots) > 0, "No applicable plots selected for the current data type (Daily/Hourly)."))
    
    # --- Build List of Plot Cards ---
    plot_card_list <- list() # Use a list to store the cards/UI elements
    
    # Summary Plot Card
    if ("summary" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          full_screen = TRUE,
          bslib::card_header("Data Summary Plot"),
          bslib::card_body(
            # MODIFICATION: Increased height from 600px to 800px
            plotOutput("dyn_summary_plot", height = "800px")
          )
        )
      ))
    }
    
    # Calendar Plots Card
    if ("calendar" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          full_screen = TRUE,
          bslib::card_header(
            class = "d-flex justify-content-between align-items-center",
            tags$span("Calendar Plots"),
            tags$button(
              class = "btn btn-sm btn-outline-secondary",
              `data-bs-toggle` = "collapse",
              `data-bs-target` = "#calendar-plots-body",
              icon("chevron-down")
            )
          ),
          bslib::card_body(
            id = "calendar-plots-body",
            class = "collapse show",
            plotOutput("dyn_calendar_plot_1", height = "600px"),
            tags$hr(),
            plotOutput("dyn_calendar_plot_2", height = "600px")
          )
        )
      ))
    }
    
    # Trend Plots Card (Conditional CPF for hourly)
    if ("trend" %in% selected_plots) { # Note: 'trend' value covers both plots
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Trend Plots"),
          bslib::card_body(
            plotOutput("dyn_trend_plot_1"), # Smooth Trend (always shown if 'trend' selected)
            if(current_mode == "hourly") {
              plotOutput("dyn_trend_plot_2") # CPF Rose
            } else {
              tags$em("CPF Rose plot requires Hourly data.") # Placeholder if daily
            }
          )
        )
      ))
    }
    
    # Theil-Sen Trend Card
    if ("theilsen" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Theil-Sen Trend (Multi-Year)"),
          bslib::card_body(
            plotOutput("dyn_theilsen_plot")
          )
        )
      ))
    }
    
    # Wind-Normalized Trend Card
    if ("windnorm" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          full_screen = TRUE,
          bslib::card_header("Wind-Normalized Pollutant Trend"),
          bslib::card_body(
            tags$p("This plot compares the original time series with a trend adjusted for wind speed, direction, and seasonality using a GAM model. This helps to identify underlying trends independent of weather variations."),
            plotOutput("dyn_windnorm_plot", height = "800px")
          )
        )
      ))
    }
    
    # Stability Analysis Card (Hourly Only)
    if ("stability" %in% selected_plots) {
      if (current_mode == "hourly") {
        plot_card_list <- c(plot_card_list, list(
          bslib::card(
            full_screen = TRUE,
            bslib::card_header("Pollutant Distribution by Atmospheric Stability"),
            bslib::card_body(
              tags$p("This plot shows pollutant concentrations under different atmospheric stability classes, which are estimated from wind speed and time of day (day/night)."),
              plotOutput("dyn_stability_plot", height = "800px")
            )
          )
        ))
      }
    }
    
    # Time Series Card
    if ("timeseries" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Time Series Plot"),
          bslib::card_body(
            plotOutput("dyn_timeseries_plot")
          )
        )
      ))
    }
    
    # ggplot Trend Card
    if ("ggtrend" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("ggplot Trend Plot"),
          bslib::card_body(
            plotOutput("dyn_ggtrend_plot", height = "800px")
          )
        )
      ))
    }
    
    # Polar Plots Card (Conditional plots for hourly)
    if ("polar" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Polar Plots"),
          bslib::card_body(
            plotOutput("dyn_polar_plot_1"), 
            if(current_mode == "hourly") plotOutput("dyn_polar_plot_2") else tags$em("CPF Polar plot requires Hourly data."),
            if(current_mode == "hourly") plotOutput("dyn_polar_plot_3") else tags$em("Day/Night Polar plot requires Hourly data."),
            plotOutput("dyn_polar_plot_4")
          )
        )
      ))
    }
    
    # Polar Annulus Card (Conditional - Hourly Only)
    if ("polarannulus" %in% selected_plots) {
      if (current_mode == "hourly") {
        plot_card_list <- c(plot_card_list, list(
          bslib::card(
            bslib::card_header("Polar Annulus Plot (by Temperature)"),
            bslib::card_body(
              plotOutput("dyn_polarannulus_plot", height = "800px")
            )
          )
        ))
      }
    }
    
    # Pollution Roses Card
    if ("polrose" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Pollution Roses"),
          bslib::card_body(
            plotOutput("dyn_polrose_plot_1"),
            plotOutput("dyn_polrose_plot_2")
          )
        )
      ))
    }
    
    # Scatter Plots (Pollutant vs Met) Card
    if ("scatter_met" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Scatter Plots (Pollutant vs Met)"),
          bslib::card_body(
            # MODIFICATION: Increased height from 350px to 700px to accommodate 2x2 grid
            plotOutput("dyn_scatter_plot_1", height="800px")
          )
        )
      ))
    }
    
    # Scatter Plots (Other) Card
    if ("scatter_other" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Scatter Plots (Other Types)"),
          bslib::card_body(
            plotOutput("dyn_scatter_plot_2"),
            plotOutput("dyn_scatter_plot_3"),
            plotOutput("dyn_scatter_plot_4")
          )
        )
      ))
    }
    
    # Correlation Plot Card
    if ("corr" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Correlation Plot"),
          bslib::card_body(
            # MODIFICATION: Added height of 600px
            plotOutput("dyn_corr_plot", height = "800px")
          )
        )
      ))
    }
    
    # TrendLevel Plots Card (Conditional - Hourly Only)
    if ("trendlevel" %in% selected_plots) {
      if (current_mode == "hourly") {
        plot_card_list <- c(plot_card_list, list(
          bslib::card(
            bslib::card_header("Trend Level Plots"),
            bslib::card_body(
              plotOutput("dyn_trendlevel_plot_1"),
              plotOutput("dyn_trendlevel_plot_2")
            )
          )
        ))
      }
    }
    
    # Time Variation / Daily Variation Card (Conditional Content)
    if ("timevar" %in% selected_plots) {
      if (current_mode == "hourly") {
        plot_card_list <- c(plot_card_list, list(
          bslib::card(
            bslib::card_header("Time Variation Plots (Hourly)"),
            bslib::card_body(
              plotOutput("dyn_timevar_plot_1", height = "650px"),
              plotOutput("dyn_timevar_plot_2", height = "650px")
            )
          )
        ))
      } else { # Daily mode
        plot_card_list <- c(plot_card_list, list(
          bslib::card(
            bslib::card_header("Daily Variation Patterns (Daily)"),
            bslib::card_body(
              plotOutput("dyn_daily_var_weekday", height = "650px"),
              plotOutput("dyn_daily_var_month", height = "650px")
            )
          )
        ))
      }
    }
    
    # Additional Plots Card (Conditional Diurnal WR for hourly)
    if ("additional" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Additional Plots"),
          bslib::card_body(
            plotOutput("dyn_additional_plot_1"),
            plotOutput("dyn_additional_plot_2"),
            if(current_mode == "hourly") plotOutput("dyn_additional_plot_3") else tags$em("Diurnal Wind Rose requires Hourly data.")
          )
        )
      ))
    }
    
    # Polar Cluster Card
    if ("polarcluster" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Polar Cluster Plot"),
          bslib::card_body(
            plotOutput("dyn_polarcluster_plot", height = "800px")
          )
        )
      ))
    }
    
    # Percentile Rose (Multiple) Card
    if ("percentilerose_multi" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Multiple Percentile Roses"),
          bslib::card_body(
            plotOutput("dyn_percentilerose_multi", height = "800px")
          )
        )
      ))
    }
    
    # Kernel Exceedance Card
    if ("kernelexceed" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Kernel Density Exceedance Plot"),
          bslib::card_body(
            plotOutput("dyn_kernelexceed_plot", height = "800px")
          )
        )
      ))
    }
    
    # Conditional Bivariate Polar Plot Card
    if ("polarconditional" %in% selected_plots) {
      plot_card_list <- c(plot_card_list, list(
        bslib::card(
          bslib::card_header("Conditional Polar Plot by Temperature"),
          bslib::card_body(
            plotOutput("dyn_polarplot_conditional", height = "800px")
          )
        )
      ))
    }
    
    # --- Final Assembly ---
    if (length(plot_card_list) == 0) {
      return(tags$p(tags$strong("No plots generated."), "Please check selections and ensure analysis was successful."))
    } else {
      return(tagList(plot_card_list))
    }
  })
  
  
  # --- Server-side Plot Rendering ---
  # Helper (renderPlot wrapper)
  # In the "Server-side Plot Rendering" section
  render_plot_safely <- function(plot_expr, plot_name = "Plot") {
    renderPlot({
      tryCatch({
        # Use the reactive VALUE `rv$merged_data`
        df <- rv$merged_data 
        sinfo <- rv$selected_site_info; 
        poll <- rv$current_pollutant_colname;
        req(df, sinfo, poll)
        validate(need(poll %in% names(df), paste("Required column '", poll, "' not found.")))
        eval(plot_expr) 
      }, error = function(e) {
        plot.new()
        title(main = paste(plot_name, "Failed"), sub = e$message, col.main = "red", cex.sub=0.8)
      })
    })
  }
  
  # --- Summary Plot ---
  output$dyn_summary_plot <- render_plot_safely(quote({
    validate(need(poll %in% names(df), "Missing pollutant"))
    
    # Correct the period argument for summaryPlot. It requires a longer period 
    # like "months" or "years" to generate its multi-panel layout. "months" is a
    # good default for data spanning several months to a few years.
    
    # Calculate data duration in days
    duration_days <- as.numeric(difftime(max(df$date, na.rm=TRUE), min(df$date, na.rm=TRUE), units = "days"))
    
    # Choose a sensible period: 'years' if more than 2 years of data, otherwise 'months'
    plot_period <- if (duration_days > 730) "years" else "months"
    
    summaryPlot(df, pollutant = poll, period = plot_period,
                main = paste("Data Summary for", poll, "-", sinfo$name_long))
  }), "SummaryPlot")
  
  # Calendar Plots (Adapted for Daily/Hourly)
  # Calendar Plot 1: Standard pollutant concentration
  # Calendar Plot 1: Standard pollutant concentration
  output$dyn_calendar_plot_1 <- render_plot_safely(quote({
    breaks <- rv$current_breaks; labels <- rv$current_labels; colors <- rv$current_colors;
    req(breaks, labels, colors)
    validate(need(poll %in% names(df), paste("Missing", poll)))
    
    start_year <- year(input$start_date_input)
    end_year <- year(input$end_date_input)
    
    generate_cal_plot <- function(target_year) {
      plot_title <- paste(sinfo$name_long, if(rv$current_datatype=="hourly") "Daily Max Hrly" else "Daily", poll,"-", target_year)
      
      if (rv$current_datatype == "hourly") {
        site_tz <- rv$selected_site_info$tz
        plot_data <- df %>% filter(lubridate::year(date) == target_year) %>% mutate(day_date = as.Date(date, tz = site_tz)) %>% group_by(day_date) %>% summarise("{poll}" := if(all(is.na(.data[[poll]]))) NA_real_ else max(.data[[poll]], na.rm = TRUE), .groups = 'drop') %>% rename(date = day_date) %>% as.data.frame()
      } else {
        plot_data <- df %>% filter(lubridate::year(date) == target_year) %>% as.data.frame()
      }
      
      validate(need(nrow(plot_data) > 0, paste("No data for calendar plot in", target_year)))
      calendarPlot(plot_data, pollutant = poll, main = plot_title, cols = colors, breaks = breaks, key.footer = paste(labels, collapse=" | "), key.position = "right")
    }
    
    if(start_year != end_year) {
      par(mfrow = c(2, 1)) 
      generate_cal_plot(start_year)
      generate_cal_plot(end_year)
    } else {
      generate_cal_plot(start_year)
    }
  }), "Cal1 (AQI Colors)")
  
  # Calendar Plot 2: Annotated with Wind Direction (Corrected for Hourly)
  output$dyn_calendar_plot_2 <- render_plot_safely(quote({
    breaks <- rv$current_breaks; labels <- rv$current_labels; colors <- rv$current_colors;
    req(breaks, labels, colors)
    validate(need(all(c(poll, "ws", "wd") %in% names(df)), "Missing pollutant, ws, or wd for wind annotation."))
    
    start_year <- year(input$start_date_input)
    end_year <- year(input$end_date_input)
    
    generate_wind_cal_plot <- function(target_year) {
      plot_title <- paste("Wind Direction on High Days -", poll, "-", target_year)
      
      plot_data <- NULL # Initialize plot_data
      
      if (rv$current_datatype == "hourly") {
        site_tz <- rv$selected_site_info$tz
        # --- NEW ROBUST AGGREGATION LOGIC FOR HOURLY DATA ---
        plot_data <- df %>%
          filter(lubridate::year(date) == target_year) %>%
          mutate(day_date = as.Date(date, tz = site_tz)) %>%
          group_by(day_date) %>%
          # Create daily summaries: max pollutant, mean ws, and mean wd
          summarise(
            "{poll}" := if(all(is.na(.data[[poll]]))) NA_real_ else max(.data[[poll]], na.rm = TRUE),
            ws = if(all(is.na(ws))) NA_real_ else mean(ws, na.rm = TRUE),
            wd = if(all(is.na(wd))) NA_real_ else mean(wd, na.rm = TRUE),
            .groups = 'drop'
          ) %>%
          # Now filter out any remaining days with incomplete data
          filter(!is.na(.data[[poll]]) & !is.na(ws) & !is.na(wd)) %>%
          rename(date = day_date) %>%
          as.data.frame()
        
      } else { # Daily data logic remains the same
        plot_data <- df %>%
          filter(lubridate::year(date) == target_year) %>%
          filter(!is.na(ws) & !is.na(wd)) %>%
          as.data.frame()
      }
      
      # Validation after filtering
      validate(need(nrow(plot_data) > 0, paste("No data with valid ws/wd found for wind calendar in", target_year)))
      
      calendarPlot(plot_data, pollutant = poll, main = plot_title, cols = colors, breaks = breaks,
                   annotate = "wd", annotate.args = list(col = "black", lwd=0.8),
                   key.footer = "Wind Vectors", key.position = "right")
    }
    
    if(start_year != end_year) {
      par(mfrow = c(2, 1))
      generate_wind_cal_plot(start_year)
      generate_wind_cal_plot(end_year)
    } else {
      generate_wind_cal_plot(start_year)
    }
  }), "Cal2 (Wind Vectors)")
  
  
  # --- Trend Plots ---
  output$dyn_trend_plot_1 <- render_plot_safely(quote({
    validate(need(poll %in% names(df), "Missing pollutant"))
    smoothTrend(df, pollutant=poll, statistic="percentile", percentile=c(5,50,95), main=paste("Trend", poll, "%iles -", sinfo$name_long, "(", rv$current_datatype, ")"))
  }), "Trend1")
  output$dyn_trend_plot_2 <- render_plot_safely(quote({
    validate(need(rv$current_datatype == "hourly", "CPF Rose requires hourly data."))
    validate(need(all(c(poll, "ws", "wd") %in% names(df)), "Missing pollutant, ws, or wd"))
    percentileRose(df, pollutant=poll, percentile=95, method="cpf", main=paste("CPF Rose (95%", poll,") -", sinfo$name_long), min.bin=5)
  }), "Trend2 (CPF Rose - Hourly Only)")
  
  # --- TheilSen Plot ---
  output$dyn_theilsen_plot <- render_plot_safely(quote({
    validate(need(poll %in% names(df),"Missing pollutant"))
    if(length(unique(year(df$date))) > 1){ TheilSen(df, pollutant=poll, deseason=TRUE, main=paste("Theil-Sen Trend", poll,"-",sinfo$name_long, "(", rv$current_datatype, ")"))
    } else { plot.new(); title("TheilSen Skipped\n(< 2 years data)") }
  }), "TheilSen")
  
  # --- Wind-Normalized Pollutant Trend ---
  output$dyn_windnorm_plot <- render_plot_safely(quote({
    validate(need(all(c(poll, "ws", "wd", "date") %in% names(df)), "Missing required variables for normalization."))
    
    model_data <- df %>%
      filter(!is.na(.data[[poll]]) & !is.na(ws) & !is.na(wd) & !is.na(date)) %>%
      # Ensure there are no duplicate timestamps, which can cause GAM to fail
      distinct(date, .keep_all = TRUE) %>%
      mutate(
        u = ws * sin(wd * pi / 180), v = ws * cos(wd * pi / 180),
        # Use numeric day of year and weekday as predictors
        jday = yday(date),
        weekday = wday(date, label = TRUE)
      )
    
    validate(need(nrow(model_data) > 100, "Wind normalization requires at least 100 complete data points."))
    
    # --- MODIFICATION START: Simpler model for daily data, more robust validation ---
    if (rv$current_datatype == "daily") {
      # A simpler model for daily data that is less prone to errors with few months
      gam_model <- gam(as.formula(paste(poll, "~ s(u, v) + s(jday, bs='cc') + weekday")), data = model_data)
    } else { # Hourly
      # Original, more complex model is fine for hourly data
      validate(need(length(unique(month(model_data$date))) > 4, "Wind normalization requires data from at least 4 different months for a stable model."))
      gam_model <- gam(as.formula(paste(poll, "~ s(u, v) + s(hour(date), bs='cc') + s(month(date), bs='cc') + year(date)")), data = model_data)
    }
    # --- MODIFICATION END ---
    
    model_data$normalized <- residuals(gam_model) + mean(model_data[[poll]], na.rm=TRUE)
    
    op <- par(mfrow = c(2, 1), mar = c(4, 4, 3, 1))
    
    plot(model_data$date, model_data[[poll]], type = "l", col = "grey70",
         main = paste("Original", poll, "Trend"), ylab = poll, xlab = "Date")
    lines(model_data$date, fitted(gam(as.formula(paste(poll, "~ s(as.numeric(date))")), data = model_data)), col="blue", lwd=2)
    legend("topright", legend=c("Original Data", "Smoothed Trend"), col=c("grey70", "blue"), lty=1, bty="n")
    
    plot(model_data$date, model_data$normalized, type = "l", col = "grey70",
         main = paste("Weather-Normalized", poll, "Trend"), ylab = paste("Normalized", poll), xlab = "Date")
    lines(model_data$date, fitted(gam(normalized ~ s(as.numeric(date)), data = model_data)), col="red", lwd=2)
    legend("topright", legend=c("Normalized Data", "Smoothed Trend"), col=c("grey70", "red"), lty=1, bty="n")
    
    par(op)
    
  }), "WindNorm")
  
  # --- Stability Analysis Plot (Hourly Only) ---
  output$dyn_stability_plot <- render_plot_safely(quote({
    # Validate that this is hourly data, as stability classes depend on time of day
    validate(need(rv$current_datatype == "hourly", "Stability analysis requires hourly data to determine day/night cycles."))
    validate(need(all(c(poll, "ws", "date") %in% names(df)), "Missing required variables (pollutant, ws, date)."))
    
    df_stability <- calculate_stability(df)
    
    # Define order for the stability classes for a logical plot layout
    stability_levels <- c("Very Unstable", "Unstable", "Slightly Unstable", "Neutral", 
                          "Slightly Stable", "Stable", "Very Stable", "Unknown")
    df_stability$stability <- factor(df_stability$stability, levels = stability_levels)
    
    # Box plot by stability class
    p <- ggplot(df_stability, aes(x = stability, y = .data[[poll]], fill = stability)) +
      geom_boxplot(na.rm = TRUE) +
      scale_fill_brewer(palette = "RdYlBu", name = "Stability Class", drop = FALSE) +
      labs(title = paste(poll, "Distribution by Atmospheric Stability"),
           x = "Stability Class", y = poll) +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            legend.position = "none") # Remove legend as fill is redundant with x-axis
    
    print(p)
    
  }), "Stability")
  
  
  # --- TimeSeries Plot (Revised for Daily using ggplot) ---
  output$dyn_timeseries_plot <- render_plot_safely(quote({
    validate(need(poll %in% names(df),"Missing pollutant"))
    
    plot_data <- NULL # Initialize
    main_title <- "" # Initialize
    units_lab <- unique(df$units_of_measure)[1] # Get units for label
    if(is.na(units_lab)) units_lab <- "(units unknown)" else units_lab <- paste0("(", units_lab, ")")
    
    if (rv$current_datatype == "hourly") {
      # Keep using timePlot for hourly (shows last month)
      end_date_hourly <- max(df$date, na.rm=TRUE)
      start_date_hourly <- floor_date(end_date_hourly, "month")
      plot_data <- df %>% filter(date >= start_date_hourly, date <= end_date_hourly)
      main_title <- paste("Hourly",poll,format(end_date_hourly,"%b %Y"),"-",sinfo$name_long)
      validate(need(nrow(plot_data) > 0, "No hourly data found for the last month"))
      timePlot(plot_data, pollutant=poll, main=main_title, ylab=paste(poll, units_lab)) # Use original timePlot
      
    } else { # Daily
      # Use ggplot for daily (shows last ~30 days)
      plot_data <- tail(df, 31)
      main_title <- paste("Daily",poll,"- Last", nrow(plot_data), "Days -",sinfo$name_long)
      validate(need(nrow(plot_data) > 0, "No daily data found for the tail period"))
      
      # Create ggplot
      p <- ggplot(plot_data, aes(x=date, y=.data[[poll]])) +
        geom_line(color="steelblue", na.rm = TRUE) +  # Draw lines connecting non-missing points
        geom_point(color="steelblue", size=1.5, na.rm = TRUE) + # Draw points
        labs(title=main_title, y=paste(poll, units_lab), x="Date") +
        theme_minimal() +
        theme(axis.text.x=element_text(angle=45, hjust=1))
      print(p) # Explicitly print the ggplot object
    }
    
  }), "TimeSeries")
  
  # --- ggplot Trend Plot ---
  output$dyn_ggtrend_plot <- render_plot_safely(quote({
    breaks<-rv$current_breaks; labels<-rv$current_labels; colors<-rv$current_colors; req(breaks, labels, colors);
    validate(need(all(c(poll,"date") %in% names(df)),"Missing poll/date")); validate(need(nrow(df)>0, "No data for ggplot"))
    units_lab <- unique(df$units_of_measure)[1]; title_prefix <- if (rv$current_datatype == "hourly") "Hourly" else "Daily"
    k_smooth <- if (rv$current_datatype == "daily") { non_na_count <- sum(!is.na(df[[poll]])); max_k <- max(3, non_na_count - 1); min(10, max_k) } else { NULL }
    p<-ggplot(df, aes(x=date, y=.data[[poll]])) + geom_point(aes(color=cut(.data[[poll]], breaks=breaks, labels=labels, include.lowest=TRUE, right=FALSE)), size=1.5, alpha=0.7) +
      scale_colour_manual(name="AQI Cat.", values=colors, na.value="grey50", drop=FALSE) +
      { if (rv$current_datatype == "hourly") { geom_smooth(method="loess", span=0.1, se=FALSE, color="black", na.rm = TRUE) } else if (!is.null(k_smooth) && k_smooth >= 3) { geom_smooth(method = "gam", formula = y ~ s(as.numeric(x), k= k_smooth), se = FALSE, color = "black", na.rm=TRUE) } else { NULL } } +
      labs(title=paste(sinfo$name_long, title_prefix, poll, "Trend", year(input$start_date_input),"-",year(input$end_date_input)), y=paste(poll, if(!is.na(units_lab)) paste0("(",units_lab,")") else ""), x="Date") + theme_minimal() + theme(axis.text.x=element_text(angle=45, hjust=1), legend.position="bottom")
    print(p)
  }), "ggTrend")
  
  # --- Polar Plots ---
  output$dyn_polar_plot_1 <- render_plot_safely(quote({ validate(need(all(c(poll,"ws","wd") %in% names(df)),"Missing poll/ws/wd")); polarPlot(df, pollutant=poll, statistic="mean", col="viridis", key.pos="bottom", key.header=paste("Mean",poll), main=paste("Mean",poll,"Conc. by Wind -", sinfo$name_long, "(", rv$current_datatype, ")")) }), "Polar1 (Mean)")
  output$dyn_polar_plot_2 <- render_plot_safely(quote({ validate(need(rv$current_datatype == "hourly", "CPF Polar plot requires hourly data.")); validate(need(all(c(poll,"ws","wd") %in% names(df)),"Missing poll/ws/wd")); polarPlot(df, pollutant=poll, statistic="cpf", percentile=90, main=paste("CPF",poll,">= 90th %ile -", sinfo$name_long), min.bin=5) }), "Polar2 (CPF - Hourly Only)")
  output$dyn_polar_plot_3 <- render_plot_safely(quote({ validate(need(rv$current_datatype == "hourly", "Day/Night Polar plot requires hourly data.")); validate(need(all(c(poll,"ws","wd") %in% names(df)),"Missing poll/ws/wd")); polarPlot(df, pollutant=poll, statistic="mean", type="daylight", cols="viridis", key.header=paste("Mean",poll), main=paste("Day vs Night Mean",poll,"-", sinfo$name_long)) }), "Polar3 (Day/Night - Hourly Only)")
  output$dyn_polar_plot_4 <- render_plot_safely(quote({ validate(need(all(c("ws","wd") %in% names(df)),"Missing ws/wd")); polarFreq(df, statistic = "frequency", main=paste("Wind Freq. by Year -", sinfo$name_long, "(", rv$current_datatype, ")"), min.bin=1) }), "Polar4 (Wind Freq)")
  # --- Polar Annulus Plot ---
  output$dyn_polarannulus_plot <- render_plot_safely(quote({
    validate(need(rv$current_datatype == "hourly", "Polar Annulus plot requires hourly data."))
    validate(need(all(c(poll, "ws", "wd", "temp") %in% names(df)), "Missing pollutant, ws, wd, or temp"))
    
    polarAnnulus(df, pollutant = poll, variable = "temp",
                 main = paste("Bivariate Polar Plot of", poll, "and Temperature -", sinfo$name_long),
                 key.header = poll)
  }), "PolarAnnulus")
  
  # --- Pollution Roses ---
  output$dyn_polrose_plot_1 <- render_plot_safely(quote({ breaks<-rv$current_breaks; colors<-rv$current_colors; req(breaks, colors); validate(need(all(c(poll,"wd","ws") %in% names(df)),"Missing poll/wd/ws")); pollutionRose(df, pollutant=poll, breaks=breaks, cols=colors, main=paste("Pollution Rose",poll,"-", sinfo$name_long, "(", rv$current_datatype, ")")) }), "PolRose1")
  output$dyn_polrose_plot_2 <- render_plot_safely(quote({ breaks<-rv$current_breaks; colors<-rv$current_colors; req(breaks, colors); validate(need(all(c(poll,"wd","ws") %in% names(df)),"Missing poll/wd/ws")); pollutionRose(df, pollutant=poll, key.header=paste("Seasonal",poll), breaks=breaks, cols=colors, type='season', main=paste("Seasonal Pollution Rose",poll,"-", sinfo$name_long, "(", rv$current_datatype, ")")) }), "PolRose2 (Seasonal)")
  
  # --- Scatter Plots ---
  output$dyn_scatter_plot_1 <- render_plot_safely(quote({
    # Define which met variables to look for based on data type
    met_vars_to_check <- if (rv$current_datatype == "hourly") {
      c("ws", "temp", "rh", "dew_point")
    } else { # daily
      c("ws", "temp", "rh", "min_dewpoint_f")
    }
    
    met_vars <- intersect(met_vars_to_check, names(df))
    validate(need(poll %in% names(df) && length(met_vars) > 0 , "Missing pollutant or required met variables."))
    
    plots_list <- list()
    title_suffix <- paste("-", sinfo$name_long, "(", rv$current_datatype, ")")
    
    # Dynamically create plots for the variables that exist in the data
    if ("ws" %in% met_vars) {
      plots_list$ws_plot <- ggplot(df, aes(x=ws, y=.data[[poll]])) + geom_point(alpha=0.3, size=1) + geom_smooth(method="gam", formula=y~s(x,k=5)) + theme_light() + labs(title=paste(poll,"vs WS"))
    }
    if ("temp" %in% met_vars) {
      plots_list$temp_plot <- ggplot(df, aes(x=temp, y=.data[[poll]])) + geom_point(alpha=0.3, size=1) + geom_smooth(method="gam", formula=y~s(x,k=5)) + theme_light() + labs(title=paste(poll,"vs Temp"))
    }
    if ("rh" %in% met_vars) {
      plots_list$rh_plot <- ggplot(df, aes(x=rh, y=.data[[poll]])) + geom_point(alpha=0.3, size=1) + geom_smooth(method="gam", formula=y~s(x,k=5)) + theme_light() + labs(title=paste(poll,"vs RH"))
    }
    # Add dew point plot conditionally
    if ("dew_point" %in% met_vars) { # Hourly
      plots_list$dew_plot <- ggplot(df, aes(x=dew_point, y=.data[[poll]])) + geom_point(alpha=0.3, size=1) + geom_smooth(method="gam", formula=y~s(x,k=5)) + theme_light() + labs(title=paste(poll,"vs Dew Point"))
    }
    if ("min_dewpoint_f" %in% met_vars) { # Daily
      plots_list$dew_plot <- ggplot(df, aes(x=min_dewpoint_f, y=.data[[poll]])) + geom_point(alpha=0.3, size=1) + geom_smooth(method="gam", formula=y~s(x,k=5)) + theme_light() + labs(title=paste(poll,"vs Min Dew Point (F)"))
    }
    
    if(length(plots_list) > 0) {
      # Adjust layout to handle up to 4 plots
      grid.arrange(grobs = plots_list, nrow = 2, top = textGrob(paste(poll,"vs Met Relationships", title_suffix), gp=gpar(fontsize=14)))
    } else {
      plot.new(); title("Scatter1 Failed\n(No Met Vars Found)")
    }
  }), "Scatter1 (Met Grid)")
  output$dyn_scatter_plot_2 <- render_plot_safely(quote({ validate(need(all(c(poll,"wd") %in% names(df)),"Missing poll/wd")); scatterPlot(df, x="wd", y=poll, method="hexbin", col="viridis", main=paste("Hexbin",poll,"vs WD -", sinfo$name_long, "(", rv$current_datatype, ")")) }), "Scatter2 (Hexbin)")
  output$dyn_scatter_plot_3 <- render_plot_safely(quote({ validate(need(all(c(poll,"temp","rh") %in% names(df)),"Missing poll/temp/rh")); scatterPlot(df, x="temp", y=poll, z="rh", col="viridis", key.title="RH (%)", main=paste("Scatter",poll,"vs Temp (col=RH) -", sinfo$name_long, "(", rv$current_datatype, ")")) }), "Scatter3 (Color=RH)")
  # Replace the existing output$dyn_scatter_plot_4 block (around line 1834-1852)
  output$dyn_scatter_plot_4 <- render_plot_safely(quote({
    validate(need(all(c(poll,"temp") %in% names(df)),"Missing poll/temp"))
    
    # Robustly clean data: ensure numeric type and remove non-finite values
    df_quant <- df %>%
      select(date, temp, all_of(poll)) %>%
      mutate(
        temp = as.numeric(temp),
        !!poll := as.numeric(.data[[poll]])
      ) %>%
      filter(is.finite(temp) & is.finite(.data[[poll]]))
    
    validate(need(nrow(df_quant) > 10,"Scatter (Quantile) failed: < 10 complete data points for analysis."))
    
    # Try-catch the scatterPlot call as it may have internal issues with certain data
    tryCatch({
      scatterPlot(df_quant, x="temp", y=poll, method="quantile", 
                  main=paste("Quantiles",poll,"vs Temp -", sinfo$name_long, "(", rv$current_datatype, ")"),
                  auto.text=FALSE)
    }, error = function(e) {
      # Fallback to a basic quantile regression plot using ggplot2
      library(quantreg)
      p <- ggplot(df_quant, aes(x=temp, y=.data[[poll]])) +
        geom_point(alpha=0.3, size=1, color="gray50") +
        geom_quantile(quantiles = c(0.1, 0.25, 0.5, 0.75, 0.9), 
                      color="blue", linewidth=0.8, alpha=0.7) +
        theme_minimal() +
        labs(title=paste("Quantiles",poll,"vs Temp -", sinfo$name_long, "(", rv$current_datatype, ")"),
             x="Temperature", y=poll)
      print(p)
    })
  }), "Scatter4 (Quantile)")
  
  # --- Correlation Plot ---
  # --- Correlation Plot ---
  output$dyn_corr_plot <- render_plot_safely(quote({
    # Define a base list of met columns
    base_met_cols <- c("ws", "wd", "temp", "rh", "pressure", "visibility", "precip")
    # Add data type-specific columns
    extra_met_cols <- if (rv$current_datatype == "hourly") {
      "dew_point"
    } else { # daily
      c("max_temp_f", "min_temp_f", "max_dewpoint_f", "min_dewpoint_f", "min_rh", "max_rh", "max_wind_speed_kts", "max_wind_gust_kts")
    }
    
    met_cols_avail <- intersect(c(base_met_cols, extra_met_cols), names(df))
    req_cols <- c("date", poll, met_cols_avail)
    
    validate(
      need(length(met_cols_avail) > 0, "Corr: No numeric met variables found."),
      need(poll %in% names(df), "Corr: Pollutant column missing.")
    )
    
    plot_cols <- c(poll, met_cols_avail)
    df_corr <- df[, req_cols]
    df_corr <- df_corr %>% mutate(across(all_of(plot_cols), as.numeric))
    
    complete_cases <- sum(complete.cases(df_corr[, plot_cols]))
    validate(need(complete_cases > 20, paste0("Corr Skipped\n(< 20 complete cases, found ", complete_cases, ")")))
    
    corPlot(df_corr, pollutants = plot_cols, type = "season", main = paste("Seasonal Corrs", poll, "& Met -", sinfo$name_long, "(", rv$current_datatype, ")"))
  }), "Corr")
  
  
  # --- TrendLevel Plots (Using Global Constants & Revised Gradient Function) ---
  
  # Reactive expression to get current breaks/colors (RECOMMENDED)
  # This avoids repeating the selection logic in each plot function
  current_palette_info <- reactive({
    # Use the DEFINED reactive value rv$current_pollutant_colname
    req(rv$current_datatype, rv$current_pollutant_colname) 
    
    # Assign the reactive value to a local variable for convenience
    local_poll_colname <- rv$current_pollutant_colname 
    
    base_breaks <- NULL
    base_colors <- NULL
    # Default label based on the column name, can be refined
    poll_label <- local_poll_colname 
    
    # Use local_poll_colname in the selection logic
    if (rv$current_datatype == "daily") {
      if (local_poll_colname == "PM25_24HR_UGM3") { 
        base_breaks <- PM25_DAILY_24HR_BREAKS_UGM3
        base_colors <- PM25_DAILY_24HR_COLS
        poll_label <- "PM₂₅ 24hr Avg (µg/m³)"
      } else if (local_poll_colname == "OZONE_8HR_PPB") { 
        base_breaks <- O3_DAILY_8HR_BREAKS_PPB
        base_colors <- O3_DAILY_8HR_COLS
        poll_label <- "O₃ 8hr Avg (ppb)"
      } # Add other daily pollutants here...
      
    } else if (rv$current_datatype == "hourly") {
      # Assuming hourly column names might differ (e.g., just "PM2.5"?)
      # Adjust these checks if your hourly column names are different!
      if (local_poll_colname == "PM2.5") { # Example: Check if it's just PM2.5 for hourly
        base_breaks <- PM25_HOURLY_BREAKS_UGM3
        base_colors <- PM25_HOURLY_COLS
        poll_label <- "PM₂₅ Hourly (µg/m³)"
      } else if (local_poll_colname == "OZONE") { # Example: Check if it's just OZONE for hourly
        base_breaks <- O3_HOURLY_BREAKS_PPM
        base_colors <- O3_HOURLY_COLS
        poll_label <- "O₃ Hourly (ppm)"
      } # Add other hourly pollutants here...
    }
    
    # Validate that breaks/colors were found
    validate(need(!is.null(base_breaks) && !is.null(base_colors),
                  paste("AQI breaks/colors not defined for:", local_poll_colname, "and type:", rv$current_datatype)))
    
    # Generate the gradient palette
    gradient_info <- generate_aqi_intra_gradient_palette_revised(
      base_breaks, base_colors, n_steps = 5 # Adjust n_steps as desired
    )
    
    # Return all necessary info
    list(
      fine_breaks = gradient_info$breaks,
      fine_colors = gradient_info$colors,
      poll_label = poll_label,
      # Return the CORRECT column name
      poll_col_name = local_poll_colname 
    )
  })
  
  
  # --- Plotting functions using the reactive expression ---
  
  # Plot 1: Month vs Hour (Hourly) or Month vs Weekday (Daily)
  output$dyn_trendlevel_plot_1 <- render_plot_safely(quote({
    palette_info <- current_palette_info() # Get breaks, colors, labels
    req(palette_info)
    
    # Validate required columns based on datatype
    req_cols <- c(palette_info$poll_col_name, "date")
    validate(need(all(req_cols %in% names(df)), paste("Missing required columns:", paste(req_cols, collapse=", "))))
    
    # Define x and y based on mode
    x_var <- "month"
    y_var <- if (rv$current_datatype == "hourly") "hour" else "weekday"
    statistic_to_use <- "mean"
    stat_desc <- if (rv$current_datatype == "hourly") "Hourly Mean" else "Daily Mean"
    y_axis_label <- if (rv$current_datatype == "hourly") "Hour of Day" else "Day of Week"
    x_axis_label <- Hmisc::capitalize(x_var)
    
    main_title <- paste(stat_desc, palette_info$poll_label, "by", x_axis_label, "&", y_axis_label, "-", sinfo$name_long)
    
    trendLevel(df,
               pollutant = palette_info$poll_col_name, # Use the correct column name
               x = x_var,
               y = y_var,
               statistic = statistic_to_use,
               main = main_title,
               ylab = y_axis_label,
               xlab = x_axis_label,
               # --- Use GRADIENT AQI settings ---
               cols = palette_info$fine_colors,
               breaks = palette_info$fine_breaks,
               key.position = "right",
               key.header = palette_info$poll_label,
               key.footer = NULL
               # --- End GRADIENT AQI settings ---
    )
  }), "TrendLvl1 (Temporal - AQI IntraGrad)")
  
  
  # Plot 2: Month vs Wind Direction
  output$dyn_trendlevel_plot_2 <- render_plot_safely(quote({
    palette_info <- current_palette_info() # Get breaks, colors, labels
    req(palette_info)
    
    # Validate required columns
    req_cols <- c(palette_info$poll_col_name, "wd", "date")
    validate(need(all(req_cols %in% names(df)), paste("Missing required columns:", paste(req_cols, collapse=", "))))
    
    
    x_var <- "month"
    y_var <- "wd"
    statistic_to_use <- "mean"
    stat_desc <- if (rv$current_datatype == "hourly") "Hourly Mean" else "Daily Mean"
    x_axis_label <- Hmisc::capitalize(x_var)
    
    main_title <- paste(stat_desc, palette_info$poll_label, "by", x_axis_label, "& Wind Dir. -", sinfo$name_long)
    
    trendLevel(df,
               pollutant = palette_info$poll_col_name, # Use the correct column name
               x = x_var,
               y = y_var,
               statistic = statistic_to_use,
               main = main_title,
               xlab = x_axis_label,
               ylab = "Wind Direction",
               # --- Use GRADIENT AQI settings ---
               cols = palette_info$fine_colors,
               breaks = palette_info$fine_breaks,
               key.position = "right",
               key.header = palette_info$poll_label,
               key.footer = NULL
               # --- End GRADIENT AQI settings ---
    )
  }), "TrendLvl2 (Month/WD - AQI IntraGrad)")
  
  # --- Time Variation Plots (Hourly Only) ---
  output$dyn_timevar_plot_1 <- render_plot_safely(quote({ validate(need(rv$current_datatype == "hourly", "Time Variation plots require hourly data.")); validate(need(poll %in% names(df),"Missing poll")); timeVariation(df, pollutant=poll, statistic="mean", conf.int=0.95, main=paste("Diurnal, Weekly, Monthly Var.",poll,"-", sinfo$name_long)) }), "TimeVar1 (Hourly Only)")
  output$dyn_timevar_plot_2 <- render_plot_safely(quote({ validate(need(rv$current_datatype == "hourly", "Time Variation plots require hourly data.")); met_var <- if ("temp" %in% names(df)) "temp" else if ("ws" %in% names(df)) "ws" else NULL; validate(need(poll %in% names(df), "Missing Pollutant"), need(!is.null(met_var), "Temp or WS missing for comparison")); timeVariation(df, pollutant=c(poll, met_var), normalise=TRUE, main=paste("Norm. Var:",poll,"vs", met_var,"-", sinfo$name_long)) }), "TimeVar2 (Norm - Hourly Only)")
  
  # --- Additional Plots ---
  # Replace the existing output$dyn_additional_plot_1 block (around line 1906-1923)
  output$dyn_additional_plot_1 <- render_plot_safely(quote({
    validate(need(all(c(poll,"temp","date") %in% names(df)),"Missing poll/temp/date"))
    
    # Robustly clean data for linear model, removing any non-finite values
    data_lr <- df %>%
      select(date, temp, all_of(poll)) %>%
      mutate(
        temp = as.numeric(temp),
        !!poll := as.numeric(.data[[poll]])
      ) %>%
      filter(is.finite(temp) & is.finite(.data[[poll]]))
    
    validate(need(nrow(data_lr) > 10, "LinRel failed: < 10 complete data points for analysis."))
    
    # Add season column if missing (linearRelation needs it when condition="season")
    if(!"season" %in% names(data_lr)) {
      data_lr <- data_lr %>%
        mutate(season = case_when(
          month(date) %in% c(12, 1, 2) ~ "winter",
          month(date) %in% c(3, 4, 5) ~ "spring",
          month(date) %in% c(6, 7, 8) ~ "summer",
          month(date) %in% c(9, 10, 11) ~ "autumn",
          TRUE ~ "unknown"
        ))
    }
    
    # Try the linearRelation plot with error handling
    tryCatch({
      linearRelation(data_lr, x="temp", y=poll, condition="season",
                     main=paste("Linear Rel",poll,"~Temp by Season -", sinfo$name_long, "(", rv$current_datatype, ")"))
    }, error = function(e) {
      # Fallback to a ggplot2 version if linearRelation fails
      p <- ggplot(data_lr, aes(x=temp, y=.data[[poll]], color=season)) +
        geom_point(alpha=0.3, size=1) +
        geom_smooth(method="lm", se=TRUE) +
        facet_wrap(~season, scales="free") +
        theme_minimal() +
        labs(title=paste("Linear Rel",poll,"~Temp by Season -", sinfo$name_long, "(", rv$current_datatype, ")"),
             x="Temperature", y=poll) +
        theme(legend.position="bottom")
      print(p)
    })
  }), "Add1 (LinRel)")
  
  output$dyn_additional_plot_2 <- render_plot_safely(quote({ validate(need(all(c("ws","wd") %in% names(df)),"Missing ws/wd")); windRose(df, main=paste("Wind Rose -", sinfo$name_long, "(", rv$current_datatype, ")")) }), "Add2 (WindRose)")
  output$dyn_additional_plot_3 <- render_plot_safely(quote({ validate(need(rv$current_datatype == "hourly", "Diurnal Wind Rose requires hourly data.")); validate(need(all(c("ws","wd") %in% names(df)),"Missing ws/wd")); windRose(df, type="hour", main=paste("Diurnal Wind Rose -", sinfo$name_long)) }), "Add3 (Diurnal WR - Hourly Only)")
  
  # --- Polar Cluster Plot ---
  output$dyn_polarcluster_plot <- render_plot_safely(quote({
    validate(need(all(c(poll, "ws", "wd") %in% names(df)), "Missing pollutant, ws, or wd"))
    
    # Perform clustering
    polarCluster(df, 
                 pollutant = poll,
                 n.clusters = 6,
                 cols = "Set1",
                 main = paste("Polar Clusters -", poll, "-", sinfo$name_long))
  }), "PolarCluster")
  
  # --- Multiple Percentile Roses ---
  output$dyn_percentilerose_multi <- render_plot_safely(quote({
    validate(need(all(c(poll, "ws", "wd") %in% names(df)), "Missing pollutant, ws, or wd"))
    
    percentileRose(df, 
                   pollutant = poll,
                   percentile = c(25, 50, 75, 90, 95),
                   type = "season",
                   main = paste("Seasonal Percentile Roses -", poll, "-", sinfo$name_long))
  }), "PercentileRoseMulti")
  
  # --- Kernel Exceedance Plot ---
  # Replace the existing output$dyn_kernelexceed_plot block (around line 1956-1973)
  output$dyn_kernelexceed_plot <- render_plot_safely(quote({
    validate(need(poll %in% names(df), "Missing pollutant"))
    
    # Define thresholds based on data type and pollutant
    threshold <- if(rv$current_datatype == "hourly") {
      if(poll == "OZONE") 0.070 else if(poll == "PM2.5") 35.4 else NULL
    } else { # daily
      if(poll == "OZONE_8HR_PPB") 70 else if(poll == "PM25_24HR_UGM3") 35.4 else NULL
    }
    
    validate(need(!is.null(threshold), paste("Threshold not defined for pollutant:", poll)))
    
    # Check if the function exists and try to use it
    if(exists("kernelExceed", where = "package:openair", mode = "function")) {
      tryCatch({
        kernelExceed(df,
                     x = poll,
                     data.thresh = threshold,
                     main = paste("Kernel Density Exceedance -", poll, ">", threshold, "-", sinfo$name_long))
      }, error = function(e) {
        # If kernelExceed fails, create a simple density plot with threshold line
        plot.new()
        plot(density(df[[poll]], na.rm = TRUE), 
             main = paste("Density Plot -", poll, "(threshold:", threshold, ")"),
             xlab = poll, ylab = "Density")
        abline(v = threshold, col = "red", lty = 2, lwd = 2)
        legend("topright", legend = paste("Threshold =", threshold), 
               col = "red", lty = 2, lwd = 2, bty = "n")
      })
    } else {
      # Function doesn't exist - create alternative visualization
      plot.new()
      title(main = "kernelExceed function not available", 
            sub = "This function may not be available in your openair version", 
            col.main = "red", cex.sub = 0.8)
    }
  }), "KernelExceed")
  
  # --- Conditional Bivariate Polar Plot ---
  output$dyn_polarplot_conditional <- render_plot_safely(quote({
    validate(need(all(c(poll, "ws", "wd", "temp") %in% names(df)), "Missing required variables"))
    
    # Create temperature categories
    df_temp <- df %>%
      mutate(temp_cat = cut(temp, 
                            breaks = quantile(temp, probs = c(0, 0.25, 0.5, 0.75, 1), na.rm = TRUE),
                            labels = c("Cold", "Cool", "Moderate", "Warm"),
                            include.lowest = TRUE))
    
    polarPlot(df_temp,
              pollutant = poll,
              type = "temp_cat",
              statistic = "mean",
              main = paste("Mean", poll, "by Temperature Category -", sinfo$name_long))
  }), "PolarConditional")
  
  # ADD THESE NEW DOWNLOAD HANDLERS:
  output$download_merged_data <- downloadHandler(
    filename = function() {
      req(rv$selected_site_info)
      paste0(rv$selected_site_info$name_short, "_", 
             rv$current_pollutant_code, "_",
             format(Sys.Date(), "%Y%m%d"), ".csv")
    },
    content = function(file) {
      req(rv$merged_data)
      write_csv(rv$merged_data, file)
    }
  )
  
  output$download_all_plots <- downloadHandler(
    filename = function() {
      req(rv$selected_site_info)
      paste0(rv$selected_site_info$name_short, "_plots_",
             format(Sys.Date(), "%Y%m%d"), ".zip")
    },
    content = function(file) {
      req(rv$plot_dir)
      plot_files <- list.files(rv$plot_dir, full.names = TRUE, 
                               pattern = paste0("\\.", input$download_format, "$"))
      if(length(plot_files) > 0) {
        zip(file, plot_files, flags = "-j")
      }
    }
  )
  
  # --- Daily Variation Plots (using ggplot) ---
  
  # Daily Variation by Weekday
  output$dyn_daily_var_weekday <- render_plot_safely(quote({
    # This plot only runs if daily mode is selected (due to UI logic)
    validate(need(rv$current_datatype == "daily", "This plot requires Daily data mode."))
    validate(need(poll %in% names(df),"Missing pollutant column"))
    
    units_lab <- unique(df$units_of_measure)[1]
    if(is.na(units_lab)) units_lab <- "(units unknown)" else units_lab <- paste0("(", units_lab, ")")
    plot_title <- paste("Mean Daily", poll, "by Day of Week -", sinfo$name_long)
    
    # Calculate mean and confidence interval per weekday
    plot_data <- df %>%
      filter(!is.na(.data[[poll]])) %>%
      mutate(weekday = lubridate::wday(date, label = TRUE, abbr = FALSE, week_start = 1)) %>% # Get weekday labels
      group_by(weekday) %>%
      summarise(
        mean_val = mean(.data[[poll]], na.rm = TRUE),
        n = n(),
        se = sd(.data[[poll]], na.rm = TRUE) / sqrt(n()),
        ci_lower = mean_val - 1.96 * se,
        ci_upper = mean_val + 1.96 * se,
        .groups = 'drop'
      ) %>%
      # Ensure factor levels are ordered correctly
      mutate(weekday = factor(weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))
    
    validate(need(nrow(plot_data) > 0, "No data to calculate weekday averages."))
    
    p <- ggplot(plot_data, aes(x = weekday, y = mean_val)) +
      geom_col(fill = "lightblue", color = "black") + # Bar chart for mean
      geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.3, color = "gray40") + # Error bars for 95% CI
      labs(title = plot_title, y = paste("Mean", poll, units_lab), x = "Day of Week") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))
    
    print(p)
  }), "DailyVar Weekday")
  
  
  # Daily Variation by Month
  output$dyn_daily_var_month <- render_plot_safely(quote({
    # This plot only runs if daily mode is selected (due to UI logic)
    validate(need(rv$current_datatype == "daily", "This plot requires Daily data mode."))
    validate(need(poll %in% names(df),"Missing pollutant column"))
    
    units_lab <- unique(df$units_of_measure)[1]
    if(is.na(units_lab)) units_lab <- "(units unknown)" else units_lab <- paste0("(", units_lab, ")")
    plot_title <- paste("Mean Daily", poll, "by Month -", sinfo$name_long)
    
    # Calculate mean and confidence interval per month
    plot_data <- df %>%
      filter(!is.na(.data[[poll]])) %>%
      mutate(month = lubridate::month(date, label = TRUE, abbr = FALSE)) %>% # Get month labels
      group_by(month) %>%
      summarise(
        mean_val = mean(.data[[poll]], na.rm = TRUE),
        n = n(),
        se = sd(.data[[poll]], na.rm = TRUE) / sqrt(n()),
        ci_lower = mean_val - 1.96 * se,
        ci_upper = mean_val + 1.96 * se,
        .groups = 'drop'
      ) %>%
      # Ensure factor levels are ordered correctly
      mutate(month = factor(month, levels = month.name)) # month.name is built-in factor levels
    
    validate(need(nrow(plot_data) > 0, "No data to calculate monthly averages."))
    
    p <- ggplot(plot_data, aes(x = month, y = mean_val)) +
      geom_col(fill = "lightgreen", color = "black") + # Bar chart for mean
      geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.3, color = "gray40") + # Error bars for 95% CI
      labs(title = plot_title, y = paste("Mean", poll, units_lab), x = "Month") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))
    
    print(p)
  }), "DailyVar Month")
  
  #STATISTICS
  # --- Reactive Calculation for Statistics ---
  stats_reactive <- reactive({
    # Use the reactive VALUE `rv$merged_data`
    req(rv$merged_data, rv$current_pollutant_colname, rv$current_datatype, input$start_date_input, input$end_date_input)
    df_stats <- rv$merged_data
    poll_col <- rv$current_pollutant_colname
    data_type <- rv$current_datatype
    start_d <- input$start_date_input
    end_d <- input$end_date_input
    
    validate(
      need(nrow(df_stats) > 0, "No merged data available for statistics."),
      need(poll_col %in% names(df_stats), paste("Pollutant column", poll_col, "not found."))
    )
    
    results <- list() # Initialize list to store results
    
    results$quality_report <- assess_data_quality(df_stats, poll_col)
    
    # --- 1. Period & Completeness ---
    duration_days <- as.numeric(difftime(end_d, start_d, units = "days")) + 1
    if (data_type == "hourly") {
      total_possible <- duration_days * 24
      time_unit <- "Hours"
    } else {
      total_possible <- duration_days
      time_unit <- "Days"
    }
    
    valid_poll <- sum(!is.na(df_stats[[poll_col]]))
    valid_ws <- if("ws" %in% names(df_stats)) sum(!is.na(df_stats$ws)) else 0
    valid_temp <- if("temp" %in% names(df_stats)) sum(!is.na(df_stats$temp)) else 0
    
    if(nrow(df_stats) > 0) results$pollutant_completeness_percent <- valid_poll / nrow(df_stats) * 100 else results$pollutant_completeness_percent <- 0
    
    results$period_summary_text <- paste(
      paste("Period:", format(start_d, "%Y-%m-%d"), "to", format(end_d, "%Y-%m-%d"), paste0("(", duration_days, " days)")),
      paste("Analysis Type:", Hmisc::capitalize(data_type)),
      paste("Total Possible", time_unit, "in Period:", total_possible),
      paste("Total Records in Merged Data:", nrow(df_stats)),
      paste("Valid Pollutant Records:", valid_poll, sprintf("(%.1f%%)", valid_poll / nrow(df_stats) * 100)),
      # Add completeness relative to *possible* periods if crucial
      # paste("Completeness vs Possible:", sprintf("%.1f%%", valid_poll / total_possible * 100)), 
      paste("Valid Wind Speed Records:", valid_ws),
      paste("Valid Temperature Records:", valid_temp),
      sep = "\n"
    )
    
    # --- 2. Descriptive Statistics ---
    desc_stats_list <- list()
    cols_to_summarize <- intersect(c(poll_col, "ws", "temp", "rh"), names(df_stats))
    
    for(col in cols_to_summarize) {
      valid_data <- df_stats[[col]][!is.na(df_stats[[col]])]
      if(length(valid_data) > 0) {
        quantiles <- quantile(valid_data, probs = c(0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.98, 0.99), na.rm = TRUE)
        desc_stats_list[[col]] <- tibble(
          Statistic = c("Mean", "Median", "Std Dev", "Min", "Max", 
                        "5th %ile", "10th %ile", "25th %ile", "75th %ile", "90th %ile", "95th %ile", "98th %ile", "99th %ile",
                        "Valid Count"),
          Value = c(mean(valid_data, na.rm = TRUE), 
                    median(valid_data, na.rm = TRUE), 
                    if(length(valid_data) > 1) sd(valid_data, na.rm = TRUE) else NA, 
                    min(valid_data, na.rm = TRUE), 
                    max(valid_data, na.rm = TRUE),
                    quantiles["5%"], quantiles["10%"], quantiles["25%"], quantiles["75%"], 
                    quantiles["90%"], quantiles["95%"], quantiles["98%"], quantiles["99%"],
                    length(valid_data)
          )
        )
      }
    }
    
    # Combine into a single table
    if (length(desc_stats_list) > 0) {
      results$desc_stats_table <- bind_rows(desc_stats_list, .id = "Parameter") %>%
        mutate(Value = round(Value, 3)) %>% # Round for display
        pivot_wider(names_from = Parameter, values_from = Value)
    } else {
      results$desc_stats_table <- tibble(Message = "No numeric variables found for descriptive stats.")
    }
    
    # --- 3. Wind Summary (Using openair helper) ---
    if (all(c("ws", "wd") %in% names(df_stats))) {
      wr_data <- tryCatch(windRose(df_stats, ws = "ws", wd = "wd", plot = FALSE), error = function(e) NULL)
      if (!is.null(wr_data)) {
        calm_freq <- wr_data$calm.freq
        mean_scalar_ws <- mean(df_stats$ws, na.rm = TRUE)
        
        results$mean_scalar_ws <- mean_scalar_ws
        
        # Vector mean (approximation without full circular stats package)
        df_stats_rad <- df_stats %>% filter(!is.na(ws) & !is.na(wd)) %>%
          mutate(wd_rad = wd * pi / 180)
        u_comp <- mean(df_stats_rad$ws * sin(df_stats_rad$wd_rad))
        v_comp <- mean(df_stats_rad$ws * cos(df_stats_rad$wd_rad))
        vector_mean_ws <- sqrt(u_comp^2 + v_comp^2)
        vector_mean_wd <- (atan2(u_comp, v_comp) * 180 / pi + 360) %% 360
        
        results$wind_summary_text <- paste(
          sprintf("Mean Scalar Wind Speed: %.2f m/s", mean_scalar_ws),
          sprintf("Frequency of Calm (<%.1f m/s): %.1f%%", wr_data$calm.ws, calm_freq),
          sprintf("Vector Mean Wind Speed: %.2f m/s", vector_mean_ws),
          sprintf("Vector Mean Wind Direction: %.1f degrees", vector_mean_wd),
          sep = "\n"
        )
      } else {
        results$wind_summary_text <- "Could not calculate wind summary (requires ws, wd)."
      }
    } else {
      results$wind_summary_text <- "Wind summary skipped (requires ws, wd)."
    }
    
    # --- 4. AQI Category Summary ---
    aqi_breaks <- rv$current_breaks
    aqi_labels <- rv$current_labels
    
    if (!is.null(aqi_breaks) && !is.null(aqi_labels) && length(aqi_labels) == length(aqi_breaks) - 1) {
      valid_poll_values <- df_stats[[poll_col]][!is.na(df_stats[[poll_col]])]
      if(length(valid_poll_values) > 0) {
        # Ensure breaks include -Inf if 0 is the first break
        if (aqi_breaks[1] == 0) aqi_breaks_cut <- c(-Inf, aqi_breaks[-1]) else aqi_breaks_cut <- aqi_breaks
        
        aqi_cats <- cut(valid_poll_values, 
                        breaks = aqi_breaks_cut, 
                        labels = aqi_labels, 
                        right = FALSE, # Intervals are [low, high)
                        include.lowest = TRUE) # Include lowest value if it matches first break
        
        results$aqi_summary_table <- table(aqi_cats, dnn = "AQI_Category") %>%
          as.data.frame() %>%
          rename(Category = AQI_Category, Count = Freq) %>%
          mutate(Percentage = round(Count / sum(Count) * 100, 1))
        
        # Add ranges for context
        ranges <- character(length(aqi_labels))
        for (i in 1:length(aqi_labels)) {
          low <- aqi_breaks[i]
          high <- aqi_breaks[i+1]
          ranges[i] <- paste0("[", round(low, 1), ", ", if(is.infinite(high)) "Inf" else round(high, 1), ")")
        }
        range_df <- data.frame(Category = factor(aqi_labels, levels=aqi_labels), Range = ranges)
        results$aqi_summary_table <- left_join(range_df, results$aqi_summary_table, by = "Category") %>%
          mutate(Count = ifelse(is.na(Count), 0, Count), # Show 0 counts
                 Percentage = ifelse(is.na(Percentage), 0, Percentage))
        
      } else {
        results$aqi_summary_table <- tibble(Message = "No valid pollutant data for AQI summary.")
      }
    } else {
      results$aqi_summary_table <- tibble(Message = "AQI breaks/labels not configured correctly.")
    }
    
    # --- 5. Hourly Specific ---
    if (data_type == "hourly" && "date" %in% names(df_stats)) {
      hourly_summary <- df_stats %>%
        filter(!is.na(.data[[poll_col]])) %>%
        mutate(hour = hour(date)) %>%
        group_by(hour) %>%
        summarise(
          Mean = mean(.data[[poll_col]], na.rm = TRUE),
          Median = median(.data[[poll_col]], na.rm = TRUE),
          Count = n(),
          .groups = 'drop'
        ) %>% arrange(hour)
      
      if(nrow(hourly_summary) > 0){
        results$hourly_diurnal_table <- hourly_summary
        peak_hour <- hourly_summary$hour[which.max(hourly_summary$Mean)]
        low_hour <- hourly_summary$hour[which.min(hourly_summary$Mean)]
        results$hourly_diurnal_text <- paste(
          paste("Hour(s) with highest mean concentration:", paste(peak_hour, collapse=", ")),
          paste("Hour(s) with lowest mean concentration:", paste(low_hour, collapse=", ")),
          sep="\n"
        )
        
        results$hourly_peaks_table <- df_stats %>%
          select(date, all_of(poll_col)) %>%
          filter(!is.na(.data[[poll_col]])) %>%
          slice_max(order_by = .data[[poll_col]], n = 20) %>%
          arrange(desc(.data[[poll_col]])) %>%
          mutate(date = format(date, "%Y-%m-%d %H:%M")) # Format for display
      } else {
        results$hourly_diurnal_table <- tibble(Message = "No valid hourly data for diurnal summary.")
        results$hourly_peaks_table <- tibble(Message = "No valid hourly data for peaks.")
        results$hourly_diurnal_text <- ""
      }
    }
    
    # --- 6. Daily Specific ---
    if (data_type == "daily" && "date" %in% names(df_stats)) {
      # Day of Week
      dow_summary <- df_stats %>%
        filter(!is.na(.data[[poll_col]])) %>%
        mutate(weekday = lubridate::wday(date, label = TRUE, abbr = FALSE, week_start=1)) %>%
        group_by(weekday) %>%
        summarise(
          Mean = mean(.data[[poll_col]], na.rm = TRUE),
          Median = median(.data[[poll_col]], na.rm = TRUE),
          Count = n(),
          .groups = 'drop'
        ) %>% 
        mutate(weekday = factor(weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))) %>%
        arrange(weekday)
      
      if(nrow(dow_summary) > 0){
        results$daily_dow_table <- dow_summary
        peak_day <- dow_summary$weekday[which.max(dow_summary$Mean)]
        low_day <- dow_summary$weekday[which.min(dow_summary$Mean)]
        results$daily_dow_text <- paste(
          paste("Day(s) with highest mean concentration:", paste(peak_day, collapse=", ")),
          paste("Day(s) with lowest mean concentration:", paste(low_day, collapse=", ")),
          sep="\n"
        )
      } else {
        results$daily_dow_table <- tibble(Message = "No valid daily data for day-of-week summary.")
        results$daily_dow_text <- ""
      }
      
      # Month
      month_summary <- df_stats %>%
        filter(!is.na(.data[[poll_col]])) %>%
        mutate(month = lubridate::month(date, label = TRUE, abbr = FALSE)) %>%
        group_by(month) %>%
        summarise(
          Mean = mean(.data[[poll_col]], na.rm = TRUE),
          Median = median(.data[[poll_col]], na.rm = TRUE),
          Count = n(),
          .groups = 'drop'
        ) %>%
        mutate(month = factor(month, levels = month.name)) %>%
        arrange(month)
      
      if(nrow(month_summary) > 0){
        results$daily_month_table <- month_summary
        peak_month <- month_summary$month[which.max(month_summary$Mean)]
        low_month <- month_summary$month[which.min(month_summary$Mean)]
        results$daily_month_text <- paste(
          paste("Month(s) with highest mean concentration:", paste(peak_month, collapse=", ")),
          paste("Month(s) with lowest mean concentration:", paste(low_month, collapse=", ")),
          sep="\n"
        )
      } else {
        results$daily_month_table <- tibble(Message = "No valid daily data for monthly summary.")
        results$daily_month_text <- ""
      }
      
      # Daily Peaks
      results$daily_peaks_table <- df_stats %>%
        select(date, all_of(poll_col)) %>%
        filter(!is.na(.data[[poll_col]])) %>%
        slice_max(order_by = .data[[poll_col]], n = 20) %>%
        arrange(desc(.data[[poll_col]])) %>%
        mutate(date = format(date, "%Y-%m-%d")) # Format for display
      
      if(nrow(results$daily_peaks_table) == 0){
        results$daily_peaks_table <- tibble(Message = "No valid daily data for peaks.")
      }
    }
    
    # --- 7. Correlation ---
    cor_cols <- intersect(c(poll_col, "ws", "temp", "rh"), names(df_stats))
    if (length(cor_cols) >= 2) {
      cor_data <- df_stats %>% select(all_of(cor_cols)) %>% na.omit()
      if(nrow(cor_data) >= 10) { # Need sufficient pairs
        cor_matrix <- cor(cor_data, use = "pairwise.complete.obs", method = "pearson")
        results$correlation_table <- as.data.frame(round(cor_matrix, 3))
      } else {
        results$correlation_table <- tibble(Message = "Insufficient complete pairs (<10) for correlation.")
      }
    } else {
      results$correlation_table <- tibble(Message = "Insufficient variables for correlation (need pollutant and at least one of ws, temp, rh).")
    }
    
    return(results)
  })
  
  # --- Render the Dynamic UI for Statistics ---
  output$statistics_dynamic_ui <- renderUI({
    # Trigger the reactive calculation
    stats_data <- stats_reactive() 
    req(stats_data)
    
    # Define common UI elements
    tagList(
      h5("Data Quality Report"),
      verbatimTextOutput("stats_quality_report"),
      tags$hr(), # Add a separator
      
      h5("Overall Period Summary"),
      verbatimTextOutput("stats_period_summary"),
      
      h5("Descriptive Statistics"),
      DT::dataTableOutput("stats_desc_table"),
      
      h5("Wind Summary"),
      verbatimTextOutput("stats_wind_summary"),
      
      h5("AQI Category Breakdown"),
      DT::dataTableOutput("stats_aqi_table"),
      
      # Conditional Panel for HOURLY Stats
      conditionalPanel(
        condition = "input.data_type == 'hourly'",
        h5("Hourly Diurnal Pattern"),
        verbatimTextOutput("stats_hourly_diurnal_text"),
        DT::dataTableOutput("stats_hourly_diurnal_table"),
        h5("Top 20 Hourly Peak Values"),
        DT::dataTableOutput("stats_hourly_peaks_table")
      ),
      
      # Conditional Panel for DAILY Stats
      conditionalPanel(
        condition = "input.data_type == 'daily'",
        h5("Daily Day-of-Week Pattern"),
        verbatimTextOutput("stats_daily_dow_text"),
        DT::dataTableOutput("stats_daily_dow_table"),
        h5("Daily Monthly Pattern"),
        verbatimTextOutput("stats_daily_month_text"),
        DT::dataTableOutput("stats_daily_month_table"),
        h5("Top 20 Daily Peak Values"),
        DT::dataTableOutput("stats_daily_peaks_table")
      ),
      
      h5("Correlation Matrix (Pollutant vs Met)"),
      DT::dataTableOutput("stats_correlation_table")
    )
  })
  
  # --- Render Specific Stat Outputs ---
  output$stats_quality_report <- renderText({
    report <- stats_reactive()$quality_report
    validate(need(report, "Generating quality report..."))
    
    if (!is.null(report$Message)) {
      return(report$Message)
    }
    
    # Build the report string line by line
    report_lines <- c(
      sprintf("Pollutant Data Completeness: %.1f%%", report$overall_completeness),
      sprintf("Number of Potential Gaps Found: %d", report$gaps),
      sprintf("Number of Extreme Outliers (3xIQR): %d", report$extreme_outliers),
      "---",
      "Meteorological Data Completeness:",
      if (!is.null(report$ws_completeness)) sprintf("  - Wind Speed (ws): %.1f%%", report$ws_completeness) else NULL,
      if (!is.null(report$wd_completeness)) sprintf("  - Wind Direction (wd): %.1f%%", report$wd_completeness) else NULL,
      if (!is.null(report$temp_completeness)) sprintf("  - Temperature (temp): %.1f%%", report$temp_completeness) else NULL,
      if (!is.null(report$rh_completeness)) sprintf("  - Relative Humidity (rh): %.1f%%", report$rh_completeness) else NULL
    )
    
    paste(report_lines, collapse = "\n")
  })
  output$stats_period_summary <- renderText({ stats_reactive()$period_summary_text })
  output$stats_wind_summary <- renderText({ stats_reactive()$wind_summary_text })
  output$stats_hourly_diurnal_text <- renderText({ stats_reactive()$hourly_diurnal_text })
  output$stats_daily_dow_text <- renderText({ stats_reactive()$daily_dow_text })
  output$stats_daily_month_text <- renderText({ stats_reactive()$daily_month_text })
  
  # Render DataTables
  output$stats_desc_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$desc_stats_table, "data.frame"), "Generating descriptive stats..."))
    DT::datatable(stats_reactive()$desc_stats_table, 
                  options = list(dom = 't', paging = FALSE, searching = FALSE, scrollX = TRUE), # Added scrollX = TRUE
                  rownames = FALSE)
  })
  output$stats_aqi_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$aqi_summary_table, "data.frame"), "Generating AQI summary..."))
    DT::datatable(stats_reactive()$aqi_summary_table, options = list(dom = 't', paging = FALSE, searching = FALSE, ordering = FALSE), rownames = FALSE)
  })
  output$stats_hourly_diurnal_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$hourly_diurnal_table, "data.frame"), "Generating hourly diurnal summary..."))
    DT::datatable(stats_reactive()$hourly_diurnal_table, options = list(pageLength = 24, searching = FALSE, ordering = FALSE), rownames = FALSE) %>% 
      DT::formatRound(columns = c('Mean', 'Median'), digits=3)
  })
  output$stats_hourly_peaks_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$hourly_peaks_table, "data.frame"), "Generating hourly peaks..."))
    DT::datatable(stats_reactive()$hourly_peaks_table, options = list(pageLength = 10, searching = FALSE, ordering = FALSE), rownames = FALSE) %>%
      DT::formatRound(columns = rv$current_pollutant_colname, digits=3)
  })
  output$stats_daily_dow_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$daily_dow_table, "data.frame"), "Generating daily DoW summary..."))
    DT::datatable(stats_reactive()$daily_dow_table, options = list(dom = 't', paging = FALSE, searching = FALSE, ordering = FALSE), rownames = FALSE) %>%
      DT::formatRound(columns = c('Mean', 'Median'), digits=3)
  })
  output$stats_daily_month_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$daily_month_table, "data.frame"), "Generating daily month summary..."))
    DT::datatable(stats_reactive()$daily_month_table, options = list(pageLength = 12, searching = FALSE, ordering = FALSE), rownames = FALSE) %>%
      DT::formatRound(columns = c('Mean', 'Median'), digits=3)
  })
  output$stats_daily_peaks_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$daily_peaks_table, "data.frame"), "Generating daily peaks..."))
    DT::datatable(stats_reactive()$daily_peaks_table, options = list(pageLength = 10, searching = FALSE, ordering = FALSE), rownames = FALSE) %>%
      DT::formatRound(columns = rv$current_pollutant_colname, digits=3)
  })
  output$stats_correlation_table <- DT::renderDataTable({
    validate(need(inherits(stats_reactive()$correlation_table, "data.frame"), "Generating correlation matrix..."))
    DT::datatable(stats_reactive()$correlation_table, options = list(dom = 't', paging = FALSE, searching = FALSE, ordering = FALSE), rownames = TRUE) # Show rownames (variable names)
  })
  
} # End Server


# =========================================================================
# --- Run App ---
# =========================================================================
shinyApp(ui = ui, server = server)