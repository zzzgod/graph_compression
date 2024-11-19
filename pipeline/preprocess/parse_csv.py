from tqdm import tqdm
import pandas as pd
import asyncio
import os
import sys

def add_project_root():
    file_path = os.path.dirname(os.path.abspath(__file__))
    while file_path != os.path.dirname(file_path):
        if file_path not in sys.path:
            sys.path.append(file_path)
        file_path = os.path.dirname(file_path)

add_project_root()

from pipeline.preprocess.parser import DarpaParser, DarpaoptcParser



class DarpaParserCSV(DarpaParser):
    def __init__(self):
        """
        Darpa日志解析器，输出csv
        """
        super().__init__()
        self.vertices = []
        self.edges = []

    async def parse_vertex(self, line, bar: tqdm = None):
        """
        解析节点
        解析单行 JSON 数据并暂存为字典
        :param line: JSON 数据
        :param bar: 进度条
        """
        # 获得解析结果
        res = await super().parse_vertex(line)
        if res:
            self.vertices.append(res)
        # 更新进度条
        if bar is not None:
            bar.update(1)

    async def parse_edge(self, line, bar: tqdm = None):
        """
        解析边
        解析单行 JSON 数据并暂存为字典
        :param line: JSON 数据
        :param bar: 进度条
        """
        # 获得解析结果
        res = await super().parse_edge(line)
        if res:
            edge1, edge2 = res
            self.edges.append(edge1)
            if edge2:
                self.edges.append(edge2)
        # 更新进度条
        if bar is not None:
            bar.update(1)

    async def save(self, save_dir: str, file_name: str, file_name2: str):
        """
        保存csv解析结果
        :return: None
        """
        # 1.转换为Dataframe
        vertices = pd.DataFrame(self.vertices)
        edges = pd.DataFrame(self.edges)
        # 2.保存
        vertices.to_csv(file_name, index=False)
        edges.to_csv(file_name2, index=False)


class DarpaoptcParserCSV(DarpaoptcParser):
    def __init__(self):
        """
        Darpa日志解析器，输出csv
        """
        super().__init__()
        self.edges = []

    async def parse_line(self, line, bar: tqdm = None):
        """
        解析边
        解析单行 JSON 数据并暂存为字典
        :param line: JSON 数据
        :param bar: 进度条
        """
        # 获得解析结果
        res = await super().parse_line(line, bar)
        if res:
            edge = res
            self.edges.append(edge)
        # 更新进度条
        if bar is not None:
            bar.update(1)

    async def save(self, save_dir: str, file_name: str):
        """
        保存csv解析结果
        :return: None
        """
        # 1.转换为Dataframe
        edges = pd.DataFrame(self.edges)
        # 2.保存
        edges.to_csv(os.path.join(save_dir, file_name), index=False)


async def main():
    parser = DarpaParserCSV()
    jsonl_file_paths = [os.path.join('../data/darpa', i) for i in os.listdir('../data/darpa')]
    save_dir = '../data/raw'
    for i in jsonl_file_paths:
        parser.edges.clear()
        parser.vertices.clear()
        name1 = i[:-6] + '_vertices.csv'
        name2 = i[:-6] + '_edges.csv'
        await parser.parse_json_file(i)
        await parser.save(save_dir, name1, name2)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # DARPA TC
    loop.run_until_complete(main())
