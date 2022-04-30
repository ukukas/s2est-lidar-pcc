librarian::shelf(magrittr, fs, terra, readr)

year <- 2018
tile <- '35VLF'

bands <- base::sprintf('*T%s_%d*_B*_20m.jp2', tile, year) %>%
    fs::dir_ls(recurse = TRUE, glob = .) %>%
    terra::rast()

base::names(bands) <- base::substr(base::names(bands), 24, 26)
terra::NAflag(bands) <- 0

scl <- base::sprintf('*T%s_%d*_SCL_20m.jp2', tile, year) %>%
    fs::dir_ls(recurse = TRUE, glob = .) %>%
    terra::rast()

masked <- terra::mask(
    x = bands,
    mask = scl,
    maskvalues = c(0:3, 7:11),
    updatevalue = 0,
    filename  = base::sprintf('com_%s_%d_20m.tif',tile, year),
    overwrite = TRUE,
    datatype = 'INT2U',
    NAflag = 0)

sdf <- base::as.data.frame(masked, xy = FALSE, cells = TRUE, na.rm = TRUE)

ldf <- base::sprintf('*grdtbl_%d_%s_20m.csv', year, tile) %>%
    fs::dir_ls(recurse = TRUE, glob = .) %>%
    readr::read_csv(col_names = TRUE,
                    col_types = 'iiiiin',
                    show_col_types = FALSE,
                    lazy = FALSE)

attributes(ldf)$spec <- NULL
attributes(ldf)$problems <- NULL

final <- dplyr::inner_join(ldf, sdf, by = 'cell') %>%
    dplyr::mutate_at(dplyr::vars(dplyr::starts_with('B')),
                     function(x) x / 10000) %T>%
    readr::write_csv(base::sprintf('regtbl_%d_%s_20m.csv', year, tile))
