#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import csv
import threading
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from dataclasses import dataclass


@dataclass
class APIConfiguration:
    """API配置数据类，用于存储API相关的配置信息"""
    endpoint_url: str = 'http://127.0.0.1:11230/v1/chat/completions'
    authorization_token: str = ""  
    model_name: str = "Qwen3-4b"
    system_prompt: str = "你是一位金融领域专家，尤其擅长分析金融问题和生成简明扼要的回答。你惜字如金，回复问题会尽量精简但不缺少重要分析，严格遵守提问人的需要格式。"
    temperature: float = 0.6
    timeout_seconds: int = 500
    max_retry_attempts: int = 5
    max_concurrent_workers: int = 1  # 测试版本：只用1个并发


class FinancialQuestionProcessor:
    """金融问题处理器类"""
    
    def __init__(self, config: APIConfiguration):
        self.config = config
        self._thread_lock = threading.Lock()
        self._session = requests.Session()
        
    def _construct_request_payload(self, question_text: str) -> Dict:
        return {
            "model": self.config.model_name,
            "messages": [
                {"role": "system", "content": self.config.system_prompt},
                {"role": "user", "content": question_text}
            ],
            "temperature": self.config.temperature,
            "extra_body": {"chat_template_kwargs": {"enable_thinking": True}}
        }
    
    def _prepare_request_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.authorization_token}",
            "Content-Type": "application/json",
        }
    
    def invoke_ai_model(self, question_content: str) -> Optional[str]:
        headers = self._prepare_request_headers()
        payload = self._construct_request_payload(question_content)
        
        try:
            response = self._session.post(
                self.config.endpoint_url,
                headers=headers,
                json=payload,
                verify=False,
                timeout=self.config.timeout_seconds
            )
            
            if response.status_code == 200:
                response_data = response.json()
                if 'choices' in response_data and len(response_data['choices']) > 0:
                    return response_data['choices'][0]['message']['content']
            return None
            
        except Exception as e:
            print(f"API调用错误: {e}")
            return None
    
    def execute_multiple_attempts(self, question_id: str, question_text: str) -> List[Dict]:
        """对单个问题执行多次尝试生成答案"""
        attempt_results = []
        
        print(f"\n开始处理问题ID: {question_id}")
        print(f"问题内容: {question_text[:100]}...")
        
        for attempt_index in range(self.config.max_retry_attempts):
            print(f"\n尝试 {attempt_index + 1}/{self.config.max_retry_attempts}")
            generated_answer = self.invoke_ai_model(question_text)
            
            result_record = {
                'id': question_id,
                'attempt': attempt_index,
                'response': generated_answer if generated_answer else "$\\boxed{模型响应异常}$"
            }
            
            attempt_results.append(result_record)
            
            # 输出当前尝试的结果
            if generated_answer:
                print(f"✅ 成功生成答案: {generated_answer[:200]}...")
            else:
                print("❌ 生成答案失败")
            
        return attempt_results


class TestBatchQuestionHandler:
    """测试版批量问题处理器 - 只处理第一个问题"""
    
    def __init__(self, processor: FinancialQuestionProcessor):
        self.question_processor = processor
        
    def _parse_input_row(self, csv_row: List[str]) -> Tuple[str, str]:
        """解析输入CSV行数据，参考evaluator.py的解析方法"""
        # 从第一列按制表符分割获取ID
        question_id = csv_row[0].split('\t')[0]
        
        # 从第一列按制表符分割获取问题内容（最后一部分）
        question_content = csv_row[0].split('\t')[-1]
        
        # 如果有多列，将后续列内容追加到问题中
        index = 1
        while index < len(csv_row):
            question_content += csv_row[index]
            index += 1
            
        return question_id, question_content
    
    def test_single_question(self, input_file_path: str = 'input.csv') -> bool:
        """测试处理单个问题"""
        output_file = 'test_result.csv'
        
        if not os.path.exists(input_file_path):
            print(f"❌ 输入文件不存在: {input_file_path}")
            return False
        
        try:
            with open(input_file_path, 'r', encoding='utf-8') as input_file, \
                 open(output_file, 'w', newline='', encoding='utf-8') as output_file_handle:
                
                csv_reader = csv.reader(input_file)
                result_writer = csv.DictWriter(
                    output_file_handle, 
                    fieldnames=['id', 'attempt', 'response']
                )
                
                # 写入CSV头部
                result_writer.writeheader()
                
                # 只处理第一行
                first_row = next(csv_reader, None)
                if first_row is None:
                    print("❌ 输入文件为空")
                    return False
                
                question_id, question_text = self._parse_input_row(first_row)
                print(f"\n📋 测试信息:")
                print(f"问题ID: {question_id}")
                print(f"问题内容: {question_text}")
                
                # 处理这一个问题
                results = self.question_processor.execute_multiple_attempts(
                    question_id, question_text
                )
                
                # 写入结果
                for result in results:
                    result_writer.writerow(result)
                
                print(f"\n🎉 测试完成！结果已保存到 {output_file}")
                return True
                
        except Exception as e:
            print(f"❌ 处理过程中发生错误: {e}")
            return False


def main():
    """测试版主函数"""
    print("🧪 开始测试单个问题处理...")
    
    # 创建API配置
    api_config = APIConfiguration()
    
    # 初始化组件
    question_processor = FinancialQuestionProcessor(api_config)
    test_handler = TestBatchQuestionHandler(question_processor)
    
    # 执行测试
    success = test_handler.test_single_question()
    
    if success:
        print("\n✅ 测试执行完成！")
    else:
        print("\n❌ 测试执行失败！")


if __name__ == "__main__":
    main()