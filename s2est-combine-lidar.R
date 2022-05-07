librarian::shelf(magrittr, parallel, doParallel, fs,
                 plyr, readr, dplyr, multidplyr)

read_files <- function(ctype, cores, year, tile) {
    cl <- parallel::makeCluster(cores, ctype)
    doParallel::registerDoParallel(cl)
    data <- base::sprintf('*grdtbl_%i_%s*_10m_*.csv', year, tile) %>%
        fs::dir_ls(recurse = TRUE, glob = .) %>%
        plyr::ldply(.fun = readr::read_csv,
                    col_names = TRUE,
                    col_types = 'iiii',
                    .parallel = TRUE,
                    .paropts = list(.packages = 'readr'),
                    .id = NULL)
    parallel::stopCluster(cl)
    base::rm(cl)
    base::gc()
    return(data)
}

combine_and_write <- function(data, cores, tresh, year, tile) {
    cl <- multidplyr::new_cluster(cores)
    data %>% dplyr::mutate(P = base::as.integer(base::ceiling(P/2)),
                            L = base::as.integer(base::ceiling(L/2))) %>%
        dplyr::group_by(P, L) %>%
        multidplyr::partition(cl) %>%
        dplyr::summarise(total = base::as.integer(base::sum(total)),
                         flag = base::as.integer(base::sum(flag))) %>%
        dplyr::collect() %>%
        dplyr::ungroup() %>%
        dplyr::slice_max(order_by = total, prop = tresh) %>%
        dplyr::mutate(pcc = flag/total) %>%
        readr::write_csv(base::sprintf('grdtbl_%i_%s_20m.csv', year, tile))
    base::rm(cl)
    base::gc()
    return(TRUE)
}

main <- function() {
    cores <- parallel::detectCores() -1
    year <- 2018
    tile <- '35VLF'
    tresh <- 1
    ctype <- 'PSOCK'
    read_files(ctype, cores, year, tile) %>%
        combine_and_write(cores, tresh, year, tile)
    return(TRUE)
}

main()
