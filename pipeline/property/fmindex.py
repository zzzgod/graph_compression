import time
from tqdm import tqdm
from typing import List
import math


class FMIndex:  
    def __init__(self, text, stop_char='\0'):  
        """  
        初始化 FM-Index，构建必要的数据结构。  

        参数:  
        text (str): 输入字符串，需以 stop_char 作为终止符号。  
        """  
        if not text.endswith(stop_char):  
            text += stop_char  # 确保文本以终止符号结束  
        self.text = text  
        print("构建后缀数组...")
        self.suffix_array = self.build_sa_qsort()  
        print("构建BWT...")
        self.bwt = self.burrows_wheeler_transform()  
        print("构建C数组...")
        self.c = self.build_c_array()  
        print("构建Occ表...")
        self.occ = self.build_occ_table()  

    def build_sa_qsort(self):
        # O(n(logn)^2)
        n = len(self.text)
        s = self.text
        rank = [ord(c) for c in s]
        sa = list(range(n))
        tmp = [0] * n
        i = 1

        # 估计总循环次数
        total_steps = int(math.log2(n)) + 1

        # 使用 tqdm 添加进度条
        with tqdm(total=total_steps, desc="构建后缀数组 (qsort)", unit="step") as pbar:
            while i < n:
                sa = sorted(sa, key=lambda x: (rank[x], rank[x + i] if x + i < n else -1))
                p = 0
                tmp[sa[0]] = 0
                for j in range(1, n):
                    prev, curr = sa[j - 1], sa[j]
                    if rank[prev] != rank[curr] or (rank[curr + i] if curr + i < n else -1) != (rank[prev + i] if prev + i < n else -1):
                        p += 1
                    tmp[curr] = p  
                rank, tmp = tmp, rank   
                if p == n - 1:
                    break
                i <<= 1
                pbar.update(1)  # 更新进度条

        return sa

    def build_sa_simple(self):  
        """  
        构建后缀数组 (Suffix Array).  

        返回:  
        list: 后缀数组。  
        """  
        suffixes = []
        for i in tqdm(range(len(self.text)), desc="构建后缀数组"):
            suffixes.append((self.text[i:], i))
        suffixes.sort(key=lambda x: x[0])
        sa = [suffix[1] for suffix in suffixes]  
        return sa  

    def burrows_wheeler_transform(self):  
        """  
        计算 Burrows-Wheeler Transform (BWT).  

        返回:  
        str: BWT 字符串。  
        """  
        bwt = []
        for i in tqdm(self.suffix_array, desc="构建BWT"):
            bwt.append(self.text[i - 1] if i != 0 else self.text[-1])
        return ''.join(bwt)

    def build_c_array(self):  
        """  
        构建 C 数组，记录每个字符在 sorted BWT 中之前所有字符的累计数量。  

        返回:  
        dict: C 数组，键为字符，值为累积数量。  
        """  
        sorted_bwt = sorted(self.bwt)  
        c = {}  
        total = 0  
        last_char = None  
        for char in tqdm(sorted_bwt, desc="构建C数组"):  
            if char != last_char:  
                c[char] = total  
                last_char = char  
            total += 1  
        return c  

    def build_occ_table(self):  
        """  
        构建 Occurrence 表，记录每个字符在 BWT 中的出现次数。  

        返回:  
        dict: Occurrence 表，键为字符，值为一个列表，表示从 BWT 开始到当前位置该字符出现的次数。  
        """  
        occ = {}  
        # 初始化每个字符对应的出现次数列表  
        for char in set(self.bwt):  
            occ[char] = [0] * (len(self.bwt) + 1)  

        # 累计每个字符的出现次数  
        for i in tqdm(range(len(self.bwt)), desc="构建Occ表"):
            char = self.bwt[i]
            for c_char in occ:  
                occ[c_char][i + 1] = occ[c_char][i]  
            occ[char][i + 1] += 1  

        return occ  

    def sais(self, text: List[int], upper: int) -> List[int]:  
        n = len(text)  
        if n == 0:  
            return []  
        if n == 1:  
            return [0]  
        if n == 2:  
            if text[0] < text[1]:  
                return [0, 1]  
            else:  
                return [1, 0]  

        # Step 1: Classify the suffixes as S or L  
        is_s = [False] * n  
        is_s[-1] = True  # Last character is S-type  
        for i in range(n - 2, -1, -1):  
            if text[i] < text[i + 1]:  
                is_s[i] = True  
            elif text[i] > text[i + 1]:  
                is_s[i] = False  
            else:  
                is_s[i] = is_s[i + 1]  

        # Step 2: Mark LMS positions  
        is_lms = [False] * n  
        for i in range(1, n):  
            if is_s[i] and not is_s[i - 1]:  
                is_lms[i] = True  

        # Step 3: Collect LMS positions  
        lms_positions = [i for i, flag in enumerate(is_lms) if flag]  

        # Step 4: Induced Sort  
        def induce_sort(lms_pos: List[int]) -> List[int]:  
            sa = [-1] * n  
            # Initialize buckets  
            bucket = [0] * (upper + 2)  
            for c in text:  
                bucket[c + 1] += 1  
            for i in range(1, len(bucket)):  
                bucket[i] += bucket[i - 1]  

            # Place LMS suffixes at the end of their buckets  
            for i in reversed(lms_pos):  
                c = text[i]  
                bucket[c + 1] -= 1  
                sa[bucket[c + 1]] = i  

            # Induce L-type  
            bucket_L = bucket.copy()  
            for i in range(n):  
                pos = sa[i] - 1  
                if pos >= 0 and not is_s[pos]:  
                    c = text[pos]  
                    sa[bucket_L[c]] = pos  
                    bucket_L[c] += 1  

            # Induce S-type  
            bucket_S = bucket.copy()  
            for i in range(n -1, -1, -1):  
                pos = sa[i] -1  
                if pos >=0 and is_s[pos]:  
                    c = text[pos]  
                    bucket_S[c +1] -=1  
                    sa[bucket_S[c +1]] = pos  

            return sa  

        sa = induce_sort(lms_positions)  

        # Step 5: Assign names to LMS substrings  
        name = 0  
        prev = -1  
        names = [-1] * n  
        for pos in sa:  
            if is_lms[pos]:  
                if prev == -1:  
                    name +=1  
                    names[pos] = name  
                    prev = pos  
                else:  
                    # Compare LMS substrings  
                    i = pos  
                    j = prev  
                    while True:  
                        if text[i] != text[j] or is_lms[i] != is_lms[j]:  
                            name +=1  
                            break  
                        i +=1  
                        j +=1  
                        if i >=n or j >=n or is_lms[i] or is_lms[j]:  
                            break  
                    names[pos] = name  
                    prev = pos  

        # Step 6: Reduce the problem  
        lms_order = [pos for pos in sa if is_lms[pos]]  
        unique = len(set([names[pos] for pos in lms_order]))  
        if unique == len(lms_order):  
            # All names are unique  
            sorted_lms = lms_order  
        else:  
            # Recursively sort LMS substrings  
            sorted_lms = self.sais([names[pos] for pos in lms_order], name)  

        # Step 7: Induce sort with sorted LMS order  
        sorted_lms_positions = [lms_order[i] for i in sorted_lms]  
        sa = induce_sort(sorted_lms_positions)  

        return sa 

    def get_bwt(self):  
        """返回 Burrows-Wheeler Transform (BWT)."""  
        return self.bwt  

    def get_suffix_array(self):  
        """返回 Suffix Array (SA)."""  
        return self.suffix_array  

    def get_c_array(self):  
        """返回 C 数组."""  
        return self.c  

    def get_occ_table(self):  
        """返回 Occurrence 表."""  
        return self.occ  

    def display_occ_table(self):  
        """打印 Occurrence 表."""  
        print("Occurrence table (occ):")  
        for char in sorted(self.occ.keys()):  
            print(f"Character '{char}': {self.occ[char]}")  


# 示例调用  
if __name__ == "__main__":  
    # 示例文本 (以终止符号 '$' 结尾)  
    stop_char = '\0'
    text = "banana" + stop_char   

    # 构建 FM-Index  
    fm_index = FMIndex(text, stop_char)  

    # 获取并打印 BWT  
    bwt = fm_index.get_bwt()  
    print("BWT:", bwt)  

    # 获取并打印后缀数组 (SA)  
    sa = fm_index.get_suffix_array()  
    print("Suffix Array (SA):", sa)  

    # 获取并打印 C 数组  
    c = fm_index.get_c_array()  
    print("C array:", c)  

    # 获取并打印 Occurrence 表  
    occ_table = fm_index.get_occ_table()  
    fm_index.display_occ_table()