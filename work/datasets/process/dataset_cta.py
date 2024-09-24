import csv
from tqdm import tqdm 
from process.dataset import GroundTruthItemInterface, GroundTruthAbstract

class GroundTruthItemCTA(GroundTruthItemInterface):
    def __init__(self, table, column, value):
        self.table: str = table
        self.column: str = column
        self.value: str = value

    @property
    def get_item(self) -> dict[str, str]:
        return {
            'table': self.table,
            'column': self.column,
        }
    
    @property
    def get_identifier(self) -> str:
        return f'{self.column}'
    
    @property
    def get_output(self) -> str:
        return f'({self.column})={self.value}'

    def __str__(self) -> str:
        return f'{self.table} {self.column} {self.value}'
    
class GroundTruthCTA(GroundTruthAbstract):

    def load(self):
        with open(self.filename, 'r') as f:
            print('Loading ground truth...')
            total_lines: int = self.number_of_items_in_csv()
            print(f'Total lines: {total_lines}')
            reader = csv.reader(f)
            for row in tqdm(reader, total=total_lines):
                current_gt: GroundTruthItemInterface = GroundTruthItemCTA(row[0], row[1], row[2])
                if current_gt.table in self.ground_truth:
                    self.ground_truth[current_gt.table][current_gt.get_identifier] = current_gt
                else:
                    self.ground_truth[current_gt.table] = {
                        current_gt.get_identifier: current_gt
                    }