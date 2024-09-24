import csv
from tqdm import tqdm 
from process.dataset import GroundTruthItemInterface, GroundTruthAbstract

class GroundTruthItemCPA(GroundTruthItemInterface):
    def __init__(self, table, col_1, col_2, value):
        self.table: str = table
        self.col_1: str = col_1
        self.col_2: str = col_2
        self.value: str = value

    def get_item(self) -> dict[str, str]:
        return {
            'table': self.table,
            'col_1': self.col_1,
            'col_2': self.col_2,
        }
    
    @property
    def get_identifier(self) -> str:
        return f'{self.col_1}_{self.col_2}'
    
    @property
    def get_output(self) -> str:
        return f'({self.col_1},{self.col_2})={self.value}'

    def __str__(self) -> str:
        return f'{self.table} {self.col_1} {self.col_2} {self.value}'
    
class GroundTruthCPA(GroundTruthAbstract):

    def load(self):
        with open(self.filename, 'r') as f:
            print('Loading ground truth...')
            total_lines: int = self.number_of_items_in_csv()
            print(f'Total lines: {total_lines}')
            reader = csv.reader(f)
            for row in tqdm(reader, total=total_lines):
                current_gt: GroundTruthItemInterface = GroundTruthItemCPA(row[0], row[1], row[2], row[3])
                if current_gt.table in self.ground_truth:
                    self.ground_truth[current_gt.table][current_gt.get_identifier] = current_gt
                else:
                    self.ground_truth[current_gt.table] = {
                        current_gt.get_identifier: current_gt
                    }