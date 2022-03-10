library(data.table)
library(lubridate)
library(tidyverse)
library(crul)
library(future.apply)
library(jsonlite)
library(foreach)
library(doParallel)
library(telegram)
registerDoParallel(cores=4)

Sys.setenv(R_TELEGRAM_BOT_RBot="5000027728:AAFRg8hucHi1ZLUMXPxTCbAukFyP4Xev1Z0")

main_url <- "https://ows.goszakup.gov.kz/"
data_path <- 'data/'

entity_list <- setNames(as.list(c("v3/trd-buy/all","v3/contract","v3/lots", "v3/subject/all")),
                        c("ads","contracts","lots","contragents"))

args <- commandArgs(trailingOnly = TRUE)


entity <- args[1]
pages <-args[2]

d <- seq(1:pages)
t <- split(d, ceiling(seq_along(d)/100))
start_time <- Sys.time()

print(paste0("Start loading ",pages, " pages of ", entity, " Start time: ", start_time))

foreach(i=1:length(t)) %dopar% {
  min_page <- min(unlist(t[i]))
  max_page <- max(unlist(t[i]))
  entity_key <-entity_list[names(entity_list)==entity]
  urls <- paste0(main_url,entity_key,"?page=",seq(min_page,max_page))
  request_list <- Async$new(urls,
                            headers =list(Authorization="Bearer 429b588ad4bbd9f473aa0e14e03479e7"),
                            opts = list(ssl_verifypeer = 0L))
  
  results <- request_list$get()
  tryCatch({
    
    df <- do.call("bind_rows", 
                  future_lapply(results, 
                                function(x) {
                                  fromJSON(x$parse("UTF-8"))$items
                                }
                                
                  )
    )
    
    fwrite(df,append = T,paste0(data_path,entity,'.csv'))
    print(paste0("bucket ", i, " out of ", length(t)," has been loaded ", Sys.time()))
  }, error=function(e) NULL)
  
}
## Create the bot object
bot <- TGBot$new(token = bot_token('RBot'))
bot$sendMessage(paste0("Loading ",pages, " of ", entity, " took ", difftime(Sys.time(),start_time)),
                chat_id = "-1001666505415")


df <- fread(file.choose())

df %>% group_by(id) %>%summarise(n=n()) %>% arrange(desc(n))



t <- df %>% filter(id==13689787)

