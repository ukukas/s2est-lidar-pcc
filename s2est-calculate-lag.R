librarian::shelf(magrittr, readr, dplyr)

read_table <- function(year, tile, res, minfrac) {
    base::sprintf('grdtbl_%i_%s_%im.csv', year, tile, res) %>%
        readr::read_csv(col_names = TRUE, col_types = 'iiiid') %>%
        dplyr::filter(total > res^2 * minfrac) %>%
        return()
}

calculate_lag <- function(table) {
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
        dplyr::summarise(lag = base::mean(pcc)) %>%
        return()
}

main <- function() {
    year <- 2018
    tile <- '35VLF'
    res <- 20
    minfrac <- 0.1
    read_table(year, tile, res, minfrac) %>%
        dplyr::inner_join(calculate_lag(.)) %>%
        readr::write_csv(
            base::sprintf('pcctbl_%i_%s_%im.csv', year, tile, res))
    return(TRUE)
}

main()
