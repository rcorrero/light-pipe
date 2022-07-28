from ..gcs import download_from_bucket, upload_to_bucket
from ..pipe import (CSVTrackerInit, DictTracker, Pipeline, PlanetPathFinder,
                    RasterLabeler, TargetRasterizer, Tiler)
from ..utils import save_to_pkl


def main():
    # bucket_name = input('GCS Bucket name: ')
    bucket_name = 'raster-training-data'
    # input_blob_path = input('Path to order directory (on Bucket): ')
    input_blob_path = 'ninheira-01/26c3199d-406c-4ce2-92b9-275e1da681ed'
    # local_dir = input('Path to local directory to store data in: ')
    local_dir = "~/data/planet_scope/ninheira_dataset_224/imagery/"

    print('Downloading data from %s...' % bucket_name)
    download_from_bucket(bucket_name, input_blob_path, local_dir)
    
    print('Tiling imagery...')
    planet_path_finder = PlanetPathFinder(local_dir)
    builder_pipe = Pipeline(transformers=[planet_path_finder])
    csv_init = CSVTrackerInit(TrackerType = DictTracker, 
                              transformer = builder_pipe)

    # fp = input('Path to item ids csv: ')
    fp = "~/data/planet_scope/ninheira_dataset_224/item_ids.csv"
    tracker = csv_init.load(fp)

    verbose = True
    
    attribute = 'class_id'
    rasterizer = TargetRasterizer(attribute=attribute, verbose=verbose)

    # tile_dir_path = input("Path to directory to store tiles in: ")
    tile_dir_path = '~/data/planet_scope/ninheira_dataset_224/tiles/'
    tile_size_x = 224
    tile_size_y = 224
    tiler = Tiler(tile_dir_path, tile_size_x, tile_size_y, verbose=verbose)

    target_value = 1 # Rasterized value derived from attribute in shp files
    raster_labeler = RasterLabeler(target_value=target_value)

    transformers = [rasterizer, tiler, raster_labeler]
    transformer_pipe = Pipeline(transformers)

    output_tracker = DictTracker()
    depth_first = True
    output_tracker = transformer_pipe.transform(tracker, output_tracker, 
                                                depth_first = depth_first)
    # tracker_save_path = input('Path to save tracker to: ')
    tracker_save_path = '~/data/planet_scope/ninheira_dataset_224/tracker.pkl'
    save_to_pkl(output_tracker, save_path = tracker_save_path)
    # output_bucket_name = input('GCS Bucket name to save tiles and tracker to: ')
    output_bucket_name = 'raster-training-data'
    # output_blob_path = input('Path to store tiles and tracker in (on Bucket): ')
    output_blob_path = 'ninheira-01/ninheira_dataset_224/tiles/'
    print('Uploading data to %s...' % output_bucket_name)
    upload_to_bucket(output_bucket_name, output_blob_path, tracker_save_path)
    upload_to_bucket(output_bucket_name, output_blob_path, tile_dir_path)
    print('Done.')


if __name__ == '__main__':
    main()
