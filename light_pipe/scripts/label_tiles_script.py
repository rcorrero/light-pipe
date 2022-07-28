from ..pipe import DictTracker, RasterLabeler
from ..utils import load_from_pkl, save_to_pkl


def main():
    tracker_path = '../../data/planet_scope_2/tracker.pkl'
    tracker = load_from_pkl(tracker_path)

    # Make transformer
    target_value = 1
    raster_labeler = RasterLabeler(target_value=target_value)

    output_tracker = DictTracker()
    output_tracker = raster_labeler.transform(input=tracker, 
                                              output=output_tracker, 
                                              depth_first=False)
    output_tracker_path = '../../data/planet_scope_2/tracker_labeled.pkl'
    save_to_pkl(output_tracker, output_tracker_path)


if __name__ == '__main__':
    main()
