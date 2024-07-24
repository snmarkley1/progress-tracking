#########################################################################
#########################################################################
###                                                                   ###
###                             PREAMBLE                              ###
###                                                                   ###
#########################################################################
#########################################################################

## PREPARE WORKSPACE
rm(list = ls()) # clear environment
options(scipen = 999)
options(digits = 6)


## Load or install packages
packages <- function(x) {
  x <- deparse(substitute(x))
  installed_packages <- as.character(installed.packages()[, 1])
  
  if (length(intersect(x, installed_packages)) == 0) {
    install.packages(pkgs = x, dependencies = TRUE, repos = "http://cran.r-project.org")
  }
  
  library(x, character.only = TRUE)
  rm(installed_packages) # Remove From Workspace
}


packages(tidyverse)
packages(tidycensus)
packages(tigris)
packages(sf)
# packages(mapview)
# packages(openxlsx)
# packages(httr2) # for rest API
# packages(here) # for file path navigation
packages(rpostgis)
packages(RPostgres)
# packages(leaflet) # for leafsync, etc.
# packages(units) # for setting units
# packages(lwgeom)  # for st_split
# packages(qgisprocess) # qgis
# packages(splitstackshape) # for cSplit
packages(tmap) # for mapping
# packages(nngeo) # for filling in polygons
# packages(rmapshaper) # for ms_simplify algorithm
# packages(waffle) # for waffle chart
# packages(collapse) # for fmax, fmean, etc.
packages(dotenv) # for putting DB details in .env file
# packages(arcgisbinding)  # for arc.write


## work in planar
sf_use_s2(FALSE)

## load env info
load_dot_env()
