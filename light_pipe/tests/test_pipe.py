import pytest

from ..pipe import *


@pytest.fixture
def make_path_item():
    paths = {
    'image_path': 'test_data/image.tif',
    'target_path': 'test_data/label/label.shp'
    }
    data = {
        'item_id': 'image'
    }
    item = PathItem(paths=paths, data=data)
    return item


@pytest.fixture
def make_dict_tracker(make_path_item):
    item = make_path_item
    tracker = DictTracker()
    tracker = tracker.set_record(item)
    return tracker


@pytest.fixture
def make_dict_tracker_with_rasterized_target():
    paths = {
        'image_path': 'test_data/image.tif',
        'target_path': 'test_data/label/label.shp',
        'rasterized_target_path': 'test_data/image_rasterized.tif'
        }
    data = {
        'item_id': 'image',
        'target_is_rasterized': True,
    }
    item = PathItem(paths=paths, data=data)
    tracker = DictTracker()
    tracker = tracker.set_record(item)
    return tracker


def test_item(make_path_item):
    item = make_path_item
    assert len(item) == 1
    for i, sub_item in enumerate(item):
        assert sub_item == item
    assert i == 0


def test_tracker(make_path_item, make_dict_tracker):
    item = make_path_item
    tracker = make_dict_tracker
    new_item = tracker.get_record('image')
    assert new_item.get_paths() == item.get_paths()
    assert new_item.get_data() == item.get_data()

    for i, t_item in enumerate(tracker):
        assert t_item.get_paths() == item.get_paths()
        assert t_item.get_data() == item.get_data()
    assert i == 0


def test_target_rasterizer(make_dict_tracker):
    tracker = make_dict_tracker
    attribute = 'class_id'

    depth_first = False
    rasterizer = TargetRasterizer(attribute=attribute)
    out_tracker = DictTracker()
    out_tracker = rasterizer.transform(tracker, depth_first=depth_first)

    raster_target_path = out_tracker.get_record('image').get_paths()['rasterized_target_path']
    ds = gdal.Open(raster_target_path)
    arr = np.array(ds.GetRasterBand(1).ReadAsArray())
    ds = None
    assert np.any(arr == 0)
    assert np.any(arr == 1)
    assert np.any(arr == 2)

    depth_first = True
    rasterizer = TargetRasterizer(attribute=attribute)
    out_gen = rasterizer.transform(tracker, depth_first=depth_first)

    for item in out_gen:
        raster_target_path = item.get_record('image').get_paths()['rasterized_target_path']
        ds = gdal.Open(raster_target_path)
        arr = np.array(ds.GetRasterBand(1).ReadAsArray())
        ds = None
        assert np.any(arr == 0)
        assert np.any(arr == 1)
        assert np.any(arr == 2)


def test_tiler(make_dict_tracker_with_rasterized_target):
    tracker = make_dict_tracker_with_rasterized_target
    tile_dir_path = "test_data/tiles/"
    tile_size_x = 128
    tile_size_y = tile_size_x
    tiler = Tiler(tile_dir_path=tile_dir_path, tile_size_x=tile_size_x,
                  tile_size_y=tile_size_y)
    depth_first = False
    out_tracker = DictTracker()
    out_tracker = tiler.transform(tracker, output=out_tracker,
                                  depth_first=depth_first)
    for item in out_tracker:
        assert item.get_data()['is_tile']

    depth_first = True
    out_gen = tiler.transform(tracker, depth_first=depth_first)
    for item in out_gen:
        assert item.get_data()['is_tile']


def test_raster_labeler(make_dict_tracker_with_rasterized_target):
    tracker = make_dict_tracker_with_rasterized_target
    target_value = 2
    labeler = RasterLabeler(target_value=target_value)

    depth_first = False
    output_tracker = DictTracker()
    output_tracker = labeler.transform(tracker, depth_first=depth_first)
    at_least_one_pos = False
    at_least_one_nonzero_pixels = False
    for item in output_tracker:
        if item.get_data()['positive_target']:
            at_least_one_pos = True
        if item.get_data()['nonzero_pixels']:
            at_least_one_nonzero_pixels = True
    assert at_least_one_pos
    assert at_least_one_nonzero_pixels

    depth_first = True
    output_gen = labeler.transform(tracker, depth_first=depth_first)
    at_least_one_pos = False
    at_least_one_nonzero_pixels = False
    for item in output_gen:
        if item.get_data()['positive_target']:
            at_least_one_pos = True
        if item.get_data()['nonzero_pixels']:
            at_least_one_nonzero_pixels = True
    assert at_least_one_pos
    assert at_least_one_nonzero_pixels
