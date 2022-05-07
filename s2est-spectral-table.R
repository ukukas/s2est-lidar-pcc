librarian::shelf(magrittr, fs, terra, readr)

get_bands <- function(tile, year) {
    bands <- base::sprintf('*T%s_%d*_B*_20m.jp2', tile, year) %>%
        fs::dir_ls(recurse = TRUE, glob = .) %>%
        terra::rast()
    base::names(bands) <- base::substr(base::names(bands), 24, 26)
    terra::NAflag(bands) <- 0
    return(bands)
}

get_scl <- function(tile, year) {
    base::sprintf('*T%s_%d*_SCL_20m.jp2', tile, year) %>%
        fs::dir_ls(recurse = TRUE, glob = .) %>%
        terra::rast() %>%
        return()
}

generate_table <- function(bands, scl, tile, year) {
    terra::mask(x = bands,
                mask = scl,
                maskvalues = c(0:3, 7:11),
                updatevalue = 0,
                filename  = base::sprintf('com_%s_%d_20m.tif',tile, year),
                overwrite = TRUE,
                datatype = 'INT2U',
                NAflag = 0) %>%
        base::as.data.frame(xy = FALSE, cells = TRUE, na.rm = TRUE) %>%
        dplyr::mutate(P = terra::colFromCell(cell),
                      L = terra::rowFromCell(cell)) %>%
        dplyr::mutate_at(dplyr::vars(dplyr::starts_with('B')),
                         function(x) x / 10000) %>%
        dplyr::select(cell, P, L, dplyr::starts_with('B')) %>%
        readr::write_csv(base::sprintf('*bndtbl_%d_%s_20m.csv', year, tile))
    return(TRUE)
}

main <- function() {
    year <- 2018
    tile <- '35VLF'
    bands <- get_bands(tile, year)
    scl <- get_scl(tile, year)
    generate_table(bands, scl, tile, year)
    return(TRUE)
}
