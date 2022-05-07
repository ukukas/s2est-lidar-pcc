librarian::shelf(magrittr, readr, dplyr, multidplyr, parallel)

read_table <- function(year, tile, res, minfrac) {
    base::sprintf('grdtbl_%i_%s_%im.csv', year, tile, res) %>%
        readr::read_csv(col_names = TRUE, col_types = 'iiiid') %>%
        dplyr::filter(total > res^2 * minfrac) %>%
        return()
}

calculate_lag <- function(table, cl) {
    table %<>% dplyr::select(P, L, pcc)
    dplyr::bind_rows(dplyr::mutate(table, P=P-1, L=L-1),
                     dplyr::mutate(table, P=P-1),
                     dplyr::mutate(table, P=P-1, L=L+1),
                     dplyr::mutate(table, L=L-1),
                     dplyr::mutate(table, L=L+1),
                     dplyr::mutate(table, P=P+1, L=L-1),
                     dplyr::mutate(table, P=P+1),
                     dplyr::mutate(table, P=P+1, L=L+1)) %>%
        dplyr::group_by(P, L) %>%
        multidplyr::partition(cl) %>%
        dplyr::summarise(lag = base::mean(pcc)) %>%
        dplyr::collect() %>%
        dplyr::ungroup() %>%
        return()
}

main <- function() {
    year <- 2018
    tile <- '35VLF'
    res <- 20
    minfrac <- 0.1
    cores <- parallel::detectCores() - 1
    cl <- multidplyr::new_cluster(cores)
    read_table(year, tile, res, minfrac) %>%
        dplyr::inner_join(calculate_lag(., cl)) %>%
        readr::write_csv(
            base::sprintf('pcctbl_%i_%s_%im.csv', year, tile, res))
    base::rm(cl)
    base::gc()
    return(TRUE)
}

main()
