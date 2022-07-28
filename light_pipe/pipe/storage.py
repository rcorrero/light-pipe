import os
from collections.abc import Iterable
from typing import Generator, Optional, Type, Union

import pandas as pd


class PathItem:
    def __init__(self, paths = None, data = None,
                 PathsStoreType: Optional[Type] = dict, 
                 DataStoreType: Optional[Type] = dict):
        self._paths = PathsStoreType()
        self._data = DataStoreType()
        if paths is not None:
            self.set_paths(paths)
        if data is not None:
            self.set_data(data)


    def get_record(self, item_id = None):
        return self


    def set_record(self, item):
        self.set_paths(item.get_paths())
        self.set_data(item.get_data())


    def get_paths(self):
        return self._paths


    def set_paths(self, paths):
        assert type(paths) == type(self._paths), ("Input type does not match"
            + " storage type.")
        self._paths = paths
        

    def get_data(self):
        return self._data


    def set_data(self, data):
        assert type(data) == type(self._data), ("Input type does not match"
            + " storage type.")
        self._data = data


    def __len__(self):
        return 1


    def __iter__(self):
        yield self


class Tracker:
    def __init__(self, BackendType):
        self._data = BackendType()


    def get_record(self, item_id) -> PathItem:
        pass


    def set_record(self, item: PathItem):
        return self


    def __len__(self):
        pass


    def __iter__(self) -> Generator:
        pass


class DictTracker(Tracker):
    def __init__(self, BackendType: Optional[Type] = dict):
        self._data = BackendType()


    def get_record(self, item_id: str) -> PathItem:
        assert item_id in self._data.keys(), "Item ID not found."
        data_dict = self._data[item_id]
        item_paths = data_dict['paths']
        item_data = data_dict['data']
        item = PathItem(paths=item_paths, data=item_data)
        return item


    def set_record(self, item: Union[PathItem, Tracker]):
        if isinstance(item, Tracker):
            for member_item in item:
                self.set_record(member_item)
            return
        item_data = item.get_data()
        item_paths = item.get_paths()
        # assert type(item_data) == dict
        # assert type(item_paths) == dict
        assert isinstance(item_data, dict)
        assert isinstance(item_paths, dict)
        assert 'item_id' in item_data.keys()
        item_id = item_data['item_id']

        self._data[item_id] = {
            'paths': item_paths,
            'data': item_data
        }
        return self


    def __len__(self):
        return len(self._data)


    def __iter__(self) -> Generator:
        for value in self._data.values():
            paths = value['paths']
            data = value['data']
            item = PathItem(paths = paths, data = data)    
            yield item


class TrackerInit:
    def __init__(self, TrackerType: Type, 
                 transformer = None):
        self.tracker = TrackerType()
        self.transformer = transformer


    def transform(self, input) -> Tracker:
        pass


class CSVTrackerInit(TrackerInit):
    def load(self, fp: os.PathLike) -> Tracker:
        '''
        Parameters
        ----------
        fp : os.PathLike
            A path to a csv with two columns
                1. target_path
                2. item_ids
            The entries in the item_ids column are expected to be 
            comma-separated strings which uniquely identify items in the 
            dataset.

        Returns
        -------
        Tracker
        '''
        item_ids_list = pd.read_csv(fp).to_dict('records')

        for row in item_ids_list:
            target_path = row['target_path'].replace("\\","/")
            item_ids_list = row['item_ids'].split(',')
            for item_id in item_ids_list:
                data = {
                    'item_id': item_id
                }
                paths = {
                    'target_path': target_path
                }
                item = PathItem(paths=paths, data=data)
                
                if self.transformer is not None:
                    item = self.transformer.transform(item)
                # if self.builders is not None:
                #     for builder in self.builders:
                #         item = builder.build(item)
                if not isinstance(item, PathItem) and isinstance(item, Iterable):
                    for sub_item in item:
                        self.tracker.set_record(sub_item)
                else:
                    assert isinstance(item, PathItem)
                    self.tracker.set_record(item)
        return self.tracker
