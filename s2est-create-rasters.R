librarian::shelf(magrittr, fs, readr, dplyr, terra)

read_template <- function(tile) {
    base::sprintf('*T%s_*_AOT_20m.jp2', tile) %>%
        fs::dir_ls(recurse = TRUE, glob = .) %>%
        terra::rast() %>%
        return()
}

get_matrix_table <- function(template, tile, year) {
    base::sprintf('grdtbl_%i_%s_20m.csv', year, tile) %>%
        readr::read_csv(col_names = TRUE, col_types = 'iiiid') %>%
        dplyr::mutate(cell = terra::cellFromRowCol(template, L, P)) %>%
        dplyr::select(cell, total, pcc) %>%
        dplyr::mutate(flag = 1) %>%
        dplyr::bind_rows(
            dplyr::filter(
                base::data.frame(cell = 1:ncell(template)),
                !(cell %in% .$cell))) %>%
        dplyr::arrange(cell) %>%
        return()
}

save_raster <- function(table, field, template, tile, year) {
    terra::setValues(x = template,
                     value = table[[field]],
                     keeptime = FALSE,
                     keepunits = FALSE,
                     props = FALSE) %>%
        terra::writeRaster(filename = base::sprintf('%s_%i_%s_20m.tif',
                                                    field, year, tile),
                           overwrite = TRUE)
    return(TRUE)
}

main <- function() {
    year <- 2018
    tile <- '35VLF'
    template <- read_template(tile)
    get_matrix_table(template, tile, year) %T>%
        save_raster('total', template, tile, year) %T>%
        save_raster('pcc', template, tile, year) %>%
        save_raster('flag', template, tile, year)
    return(TRUE)
}
