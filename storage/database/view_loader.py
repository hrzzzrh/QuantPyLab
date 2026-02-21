import importlib.util
import inspect
from pathlib import Path
from typing import List, Type, Dict
from graphlib import TopologicalSorter
from .view_base import DuckDBView

class ViewLoader:
    """
    视图加载器：负责扫描、排序及可视化 DuckDB 视图。
    """

    def __init__(self, views_dir: Path):
        self.views_dir = views_dir
        self.view_classes: Dict[str, Type[DuckDBView]] = {}

    def discover_views(self):
        """递归扫描并导入所有视图类"""
        self.view_classes = {}
        for py_file in self.views_dir.rglob("*.py"):
            if py_file.name == "__init__.py":
                continue
            
            # 动态导入模块
            module_name = f"storage.database.views.{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec and spec.loader:
                try:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    # 寻找 DuckDBView 的子类
                    for name, obj in inspect.getmembers(module):
                        if inspect.isclass(obj) and issubclass(obj, DuckDBView) and obj is not DuckDBView:
                            view_instance = obj()
                            self.view_classes[view_instance.name] = obj
                except Exception:
                    from utils.logger import logger
                    logger.warning(f"导入视图文件失败 {py_file}", exc_info=True)

    def get_sorted_views(self) -> List[DuckDBView]:
        """使用拓扑排序获取创建顺序"""
        ts = TopologicalSorter()
        instances = {name: cls() for name, cls in self.view_classes.items()}
        
        for name, instance in instances.items():
            ts.add(name, *instance.dependencies)
        
        try:
            order = list(ts.static_order())
            # 只返回我们定义的视图实例（排除可能的外部依赖）
            return [instances[name] for name in order if name in instances]
        except Exception as e:
            raise ValueError("视图依赖图中检测到循环引用或错误: %s" % e)

    def generate_puml(self) -> str:
        """生成 PlantUML 依赖图源码"""
        lines = ["@startuml", "skinparam componentStyle uml2", "title DuckDB View Dependencies", ""]
        
        # 定义组件
        instances = {name: cls() for name, cls in self.view_classes.items()}
        for name in sorted(instances.keys()):
            lines.append("[%s]" % name)
            
        # 定义关系
        for name, instance in instances.items():
            for dep in instance.dependencies:
                lines.append("[%s] --> [%s]" % (dep, name))
                
        lines.append("@enduml")
        return "\n".join(lines)
