library(RCurl);
library(httr)
library(urltools)
library(dplyr)
library(tidyr)
library(stringr)
library(RPostgreSQL)
library(foreach)
library(sqldf)

options(RCurlOptions = list(verbose = FALSE, capath = system.file("CurlSSL", package = "RCurl"), ssl.verifypeer = FALSE))

#-----------------------------------------------------------------------------------------------------------#
#   Open db connection                                                                                      #
#-----------------------------------------------------------------------------------------------------------#
pg <- dbDriver("PostgreSQL")
circle_db <- dbConnect(pg,host="localhost", dbname="circle",user="postgres",password="x.2626.t")

#----------------  Artist name and twitter handle   --------------------------------------#
artist_table <- dbGetQuery(circle_db,"select artist_id, lower(artist) as artist, artist as dbartist, nbs_id from artists")
artist_online_table <- dbGetQuery(circle_db,"select * from artists_online")

#----------------------   Filter artist who are not in NBS db   -------------------------------#
artist_sample <- filter(artist_table,is.na(nbs_id))

#--------------------------------------------------------------------------------------------#
#   Set up data frames to hold values from NBS API call                                      #
#--------------------------------------------------------------------------------------------#
artist_id <- data.frame(artist=character(0), nbs_id=character(0), stringsAsFactors = FALSE)
platforms <- data.frame(artist=character(0), platform=character(0),plaform_url=character(0),stringsAsFactors = FALSE)

artist_id_cols <- c("artist","nbs_id")
artist_id_vals <- c("NULL","NULL")
i_artist_id_holder <- data.frame(artist_id_cols,artist_id_vals, stringsAsFactors = FALSE)
i_artist_id <- distinct(spread(i_artist_id_holder,artist_id_cols,artist_id_vals,fill="NULL")) 

platforms_cols <- c("artist","platform","platform_url")
platforms_vals <- c("NULL","NULL","NULL")
i_platforms_holder <- data.frame(platforms_cols,platforms_vals, stringsAsFactors = FALSE)
i_platforms <- distinct(spread(i_platforms_holder,platforms_cols,platforms_vals,fill="NULL")) 


#--------------------------------------------------------------------------------------------#
#   Search on artist name                                                                    #
#--------------------------------------------------------------------------------------------#
foreach(y=1:nrow(artist_sample)) %do% {
  
    #--------------   URLencode the artist term  ------------------------------------------------#
    artist_encode <- URLencode(artist_sample$artist[y])
    nbs_search_query <- paste0("http://blastro.api3.nextbigsound.com/artists/search.json?q=",artist_encode)
    nbs_search <- GET(nbs_search_query, accept_json())

    #--------------------------------------------------------------------------------------------#
    #   Check to see if the artist exists in the Next Big Sound database                         #
    #--------------------------------------------------------------------------------------------#
    nbs_id <- names(content(nbs_search)[1])
    if(nbs_id == 'status') {
      
      print(paste(artist_sample$artist[y],"is not found in NBS data base"))
      
    } else {
      #---------------------------------------------------------------------------------------------#
      #   Data frame for the artists in the NBS database                                            #
      #---------------------------------------------------------------------------------------------#
      
      i_artist_id$artist <- artist_sample$artist[y]
      i_artist_id$nbs_id <- nbs_id
      
      current_artist_id <- select(i_artist_id,artist,nbs_id)
      artist_id <- rbind(artist_id,current_artist_id)
      
    }

}

#--------------------------------------------------------------------------------------------#
#   Clean up data with node nbs ids                                                          #
#--------------------------------------------------------------------------------------------#
artist_id <- filter(artist_id, nbs_id != 'node')

#--------------------------------------------------------------------------------------------#
#   Artist profile metrics                                                                   #
#--------------------------------------------------------------------------------------------#
foreach(x=1:nrow(artist_id)) %do% {
  
    nbs_id_query <- paste0("http://blastro.api3.nextbigsound.com/metrics/artist/",artist_id$nbs_id[x],".json")
    nbs_req <- GET(nbs_id_query, accept_json())

    #----------------------------------------------------------------------------------------#
    #   Gather online profiles for the artist                                                #
    #----------------------------------------------------------------------------------------#
    foreach(xx=1:length(content(nbs_req))) %do% {
      i_platforms$artist <- artist_id$artist[x]
      i_platforms$platform <- content(nbs_req)[[xx]][1]$Service$name
      i_platforms$platform_url <- content(nbs_req)[[xx]][2]$Profile$url
      
      #---------------------------------------------------------------------------------------------#
      #   Data frame for the NBS artist profiles                                                    #
      #---------------------------------------------------------------------------------------------#

      current_platforms <- select(i_platforms,artist,platform,platform_url)
      platforms <- rbind(platforms,current_platforms)
     
    }   #--------------   End Loop for profile data frame construction   -------------------#
    
}   #------------------   End Loop for NBS artist profile API call  -------------------------#

#--------------------------------------------------------------------------------------------#
#   Put data frames together                                                                 #
#--------------------------------------------------------------------------------------------#

#----------------   Merge platforms with db artist table     ---------------------------------#
artist_platform <- sqldf("select a.dbartist as artist, lower(p.platform) as platform, p.platform_url as link from artist_table a, platforms p where a.artist = p.artist",drv="SQLite")

#----------------------------------------------------------------------------------------------#
#   Check for profiles.  If one exists, update record.   Otherwise insert a new record         #
#----------------------------------------------------------------------------------------------#

#------------    Existing Records   ------------------------------------#
artists_online_existing <- sqldf("select o.artist, o.platform, p.link from artist_online_table o, artist_platform p where o.artist = p.artist and o.platform = p.platform",drv="SQLite")

#---------------  New Records  ------------------------------------------#
artist_online_new <- anti_join(artist_platform,artists_online_existing,by=c("artist", "platform"))

#--------------------------------------------------------------------------------#
#  Replace single quotes in artist name to insert in database                    #
#--------------------------------------------------------------------------------#
artist_online_clean <- str_replace_all(fixed(artist_online_new$artist),"'", "''")

#-----------------   Insert New Records in artists_online tables   ---------------------------------------#
if (nrow(artist_online_new) > 0 ) {
  
  for (i in 1:nrow(artist_online_new)) {
     #--- for (i in 1:2) {
    
    equery <- paste0("INSERT INTO artists_online (artist,platform,link) VALUES ('",artist_online_clean[i],"','",artist_online_new$platform[i],"','",artist_online_new$link[i],"')")
    
    
    #--- print(equery)
    pginsert <- dbSendQuery(circle_db,equery)
  }
}


#-------------------   Warning message in log   -------------------------------------------------#
if (nrow(artist_platform) != (nrow(artists_online_existing)+nrow(artist_online_new))) print("Warning: artist_online insert/updates records length mismatch")


#==================================================================================================#
#   Add Next Big Sound Id to the artists table                                                     #
#==================================================================================================#

nbs_insert <- sqldf("select a.artist_id, i.artist, i.nbs_id from artist_id i, artist_table a where a.artist = i.artist",drv="SQLite")

#-----------------   Insert New Records in artists_online tables   ---------------------------------------#
if (nrow(nbs_insert) > 0 ) {
  
  for (i in 1:nrow(nbs_insert)) {
    #-- for (i in 1:2) {
    
    equery <- paste0("UPDATE artists set nbs_id = ",nbs_insert$nbs_id[i]," where artist_id = ",nbs_insert$artist_id[i])
    #---print(equery)
    pginsert <- dbSendQuery(circle_db,equery)
  }
}






