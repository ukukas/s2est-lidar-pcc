librarian::shelf(magrittr, readr, dplyr, parallel, multidplyr)

read_table <- function(year, tile, res, minfrac) {
    base::sprintf('pcctbl_%i_%s_%im.csv', year, tile, res) %>%
        readr::read_csv(col_names = TRUE, col_types = 'iiiid') %>%
        dplyr::filter(total > res^2 * minfrac) %>%
        return()
}

get_lag <- function(table, dist, refP, refL){
    table %>%
        dplyr::filter(dplyr::between(P, refP-dist, refP+dist),
                  dplyr::between(L, refL-dist, refL+dist),
                  !(P==refP & L==refL)) %>%
        dplyr::pull(pcc) %>%
        base::mean() %>%
        return()
}

calculate_lag <- function(table, dist, cores, lagfunc) {
    cl <- multidplyr::new_cluster(cores)
    multidplyr::cluster_library(cl, c('magrittr', 'dplyr'))
    multidplyr::cluster_copy(cl, c('lagfunc', 'table'))
    table %>%
        dplyr::rowwise() %>%
        multidplyr::partition(cl) %>%
        dplyr::mutate(lag = lagfunc(table, P, L)) %>%
        dplyr::collect() %>%
        dplyr::ungroup() %>%
        return()
}

main <- function() {
    year <- 2018
    tile <- '35VLF'
    res <- 20
    minfrac <- 0.1
    dist <- 1
    cores <- parallel::detectCores() - 1
    read_table(year, tile, res, minfrac) %>%
        calculate_lag(dist, cores, get_lag) %>%
        readr::write_csv(
            base::sprintf('pcctbl_%i_%s_%im.csv', year, tile, res))
    return(TRUE)
}

main()
