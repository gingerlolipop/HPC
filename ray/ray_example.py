#@ray_example.py

import ray
import time

# 初始化 Ray
ray.init()

##################################################################################
# 1. 松散耦合示例：独立计算平方 - 每个任务独立地计算一个数字的平方，彼此之间没有任何联系。
##################################################################################

@ray.remote
def compute_square(x):
    time.sleep(1)  # 模拟耗时计算
    return x * x

# 同时启动多个独立任务
loose_tasks = [compute_square.remote(i) for i in range(10)]
loose_results = ray.get(loose_tasks)
print("松散耦合结果（各数的平方）:", loose_results)


##########################################################################################
# 2. 紧密耦合示例：共享累加器 - 每个任务计算一个数字的平方，然后通过共享的 Actor 将这些平方加起来
##########################################################################################

# 定义一个 Actor 类，用于在多个任务间共享状态
@ray.remote
class SumActor:
    def __init__(self):
        self.total = 0

    def add(self, value):
        self.total += value
        return self.total

    def get_total(self):
        return self.total

# 创建共享的累加器实例
sum_actor = SumActor.remote()

@ray.remote
def compute_and_add(x, actor):
    result = x * x
    time.sleep(1)  # 模拟耗时计算
    # 将结果添加到共享累加器中（紧密耦合，多个任务需要访问同一个 Actor）
    updated_total = ray.get(actor.add.remote(result))
    return result, updated_total

# 启动多个任务，它们在计算完各自的平方后，把结果累加到共享累加器中
tight_tasks = [compute_and_add.remote(i, sum_actor) for i in range(10)]
tight_results = ray.get(tight_tasks)
print("紧密耦合结果（每个任务返回自己的平方和当前累计值）:")
for idx, (square, total) in enumerate(tight_results):
    print(f"  任务 {idx}: {square}，累计值: {total}")

# 输出累计结果
final_total = ray.get(sum_actor.get_total.remote())
print("最终累计值:", final_total)
