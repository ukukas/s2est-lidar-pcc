librarian::shelf(magrittr, parallel, doParallel, fs, plyr, readr, dplyr,
                 multidplyr, terra)

cores <- parallel::detectCores() - 1
year <- 2018
tile <- '35VLF'
tresh <- 0.9

cl <- parallel::makePSOCKcluster(cores)
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

cl <- multidplyr::new_cluster(cores)

data %<>% dplyr::mutate(P = base::as.integer(base::ceiling(P/2)),
                        L = base::as.integer(base::ceiling(L/2))) %>%
    dplyr::group_by(P, L) %>%
    multidplyr::partition(cl) %>%
    dplyr::summarise(total = base::as.integer(base::sum(total)),
                     flag = base::as.integer(base::sum(flag))) %>%
    dplyr::collect() %>%
    dplyr::ungroup() %>%
    dplyr::slice_max(order_by = total, prop = tresh) %>%
    dplyr::mutate(pcc = flag/total)

base::rm(cl)
base::gc()

template <- base::sprintf('*T%s_*_AOT_20m.jp2', tile) %>%
    fs::dir_ls(recurse = TRUE, glob = .) %>%
    terra::rast()

data %<>% dplyr::mutate(cell = terra::cellFromRowCol(template, L, P)) %>%
    dplyr::select(cell, P, L, total, flag, pcc) %T>%
    readr::write_csv(base::sprintf('grdtbl_%i_%s_20m.csv', year, tile))

full <- data %>%
    dplyr::select(cell, total, pcc) %>%
    dplyr::mutate(flag = 1) %>%
    dplyr::bind_rows(
        dplyr::filter(
            base::data.frame(cell = 1:ncell(template)),
            !(cell %in% .$cell))) %>%
    dplyr::arrange(cell)

tot <- terra::setValues(x = template,
                        value = full$total,
                        keeptime = FALSE,
                        keepunits = FALSE,
                        props = FALSE)

pcc <- terra::setValues(x = template,
                        value = full$pcc,
                        keeptime = FALSE,
                        keepunits = FALSE,
                        props = FALSE)

flg <- terra::setValues(x = template,
                        value = full$flag,
                        keeptime = FALSE,
                        keepunits = FALSE,
                        props = FALSE)

terra::writeRaster(x = tot,
                   filename = base::sprintf('tot_%i_%s_20m.tif', year, tile),
                   overwrite = TRUE)

terra::writeRaster(x = pcc,
                   filename = base::sprintf('pcc_%i_%s_20m.tif', year, tile),
                   overwrite = TRUE)

terra::writeRaster(x = flg,
                   filename = base::sprintf('flg_%i_%s_20m.tif', year, tile),
                   overwrite = TRUE)
