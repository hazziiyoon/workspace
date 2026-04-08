import polars as pl
from pyvis.network import Network
import networkx as nx
import os

# 1. 경로 설정
GRAPH_PATH = "block1_co_purchase_graph.parquet"
B4_PATH = "/Users/hajiyoon/dataset_seveneleven/B4_food_item_data.parquet"
OUTPUT_HTML = "co_purchase_network.html"

def generate_visualization(top_n=300):
    print(f"🚀 {top_n}개의 핵심 연결 추출 및 고도화된 시각화 시작...")

    # [Step 1] 데이터 로드
    graph_df = pl.read_parquet(GRAPH_PATH)
    b4_df = pl.read_parquet(B4_PATH).select([
        pl.col("ITEM_CD").cast(pl.String).alias("code"),
        pl.col("ITEM_NM").alias("name")
    ])

    # [Step 2] 상품명 매핑 (Join)
    graph_named = graph_df.join(b4_df, left_on="source", right_on="code", how="left").rename({"name": "source_name"})
    graph_named = graph_named.join(b4_df, left_on="target", right_on="code", how="left").rename({"name": "target_name"})

    graph_named = graph_named.with_columns([
        pl.col("source_name").fill_null(pl.col("source")),
        pl.col("target_name").fill_null(pl.col("target"))
    ])

    # [Step 3] 상위 N개 추출
    top_edges = graph_named.sort("weight", descending=True).head(top_n)
    
    # [Step 4] NetworkX를 사용하여 차수(Degree) 계산
    G = nx.Graph()
    for row in top_edges.to_dicts():
        G.add_edge(row["source_name"], row["target_name"], weight=row["weight"])
    
    degrees = dict(G.degree())
    
    # [Step 5] Pyvis 네트워크 생성
    # cdn_resources='remote'를 사용하여 로컬에서 라이브러리 파일 누락 방지
    net = Network(height="800px", width="100%", bgcolor="#1a1a1a", font_color="white", notebook=False)
    
    # 가중치 정규화 (시각화 두께용)
    max_w = top_edges["weight"].max()
    
    for node, deg in degrees.items():
        # 노드 툴팁 구성 (HTML 활용)
        title = f"""
        <div style="color: white; background: #333; padding: 5px; border-radius: 5px;">
            <b>상품명:</b> {node}<br>
            <b>노드 차수(Degree):</b> {deg}
        </div>
        """
        # 노드 크기를 차수에 비례하게 설정
        size = 10 + (deg * 2)
        net.add_node(node, label=node, title=title, size=size, color="#4AC7E0")

    for edge in G.edges(data=True):
        src, dst, data = edge
        weight = data["weight"]
        # 엣지 툴팁 구성
        title = f"연결 강도(Weight): {weight:,.2f}"
        # 두께 설정
        width = (weight / max_w) * 15 + 1
        net.add_edge(src, dst, value=weight, width=width, title=title, color="rgba(200,200,200,0.3)")

    # [Step 6] 고도화된 옵션 설정 (물리 엔진 및 인터랙션)
    options = {
      "interaction": {
        "hover": True,
        "multiselect": True,
        "tooltipDelay": 200,
        "hideEdgesOnDrag": True,
        "navigationButtons": True
      },
      "nodes": {
        "font": {"size": 14, "face": "Pretendard, sans-serif"},
        "borderWidth": 2,
        "color": {
          "border": "#2B7CE9",
          "background": "#97C2FC",
          "highlight": {"background": "#FFA500", "border": "#FF8C00"}
        }
      },
      "edges": {
        "color": {"inherit": True},
        "smooth": {"type": "continuous", "roundness": 0.5}
      },
      "physics": {
        "barnesHut": {
          "avoidOverlap": 1,
          "gravitationalConstant": -80000,
          "centralGravity": 0.3,
          "springLength": 100,
          "springConstant": 0.05,
          "damping": 0.1
        },
        "stabilization": {
          "enabled": True,
          "iterations": 2000,
          "updateInterval": 50
        }
      }
    }
    
    import json
    net.set_options(json.dumps(options))

    # [Step 7] 하이라이트/디밍용 커스텀 JS 주입
    # html_content를 생성한 뒤 마지막에 스크립트 추가
    path = OUTPUT_HTML
    net.save_graph(path)
    
    # 생성된 HTML에 하이라이트 스크립트 추가
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Vis.js의 이벤트를 처리하는 스크립트
    highlight_js = """
    <script type="text/javascript">
    network.on("selectNode", function (params) {
        if (params.nodes.length > 0) {
            var selectedNode = params.nodes[0];
            var connectedNodes = network.getConnectedNodes(selectedNode);
            var allNodes = nodes.get();
            var allEdges = edges.get();
            
            connectedNodes.push(selectedNode);
            
            var updateNodes = allNodes.map(function(node) {
                if (connectedNodes.indexOf(node.id) !== -1) {
                    node.color = {opacity: 1};
                    node.font = {color: "white", opacity: 1};
                } else {
                    node.color = {opacity: 0.1};
                    node.font = {color: "rgba(255,255,255,0.1)", opacity: 0.1};
                }
                return node;
            });
            nodes.update(updateNodes);
            
            var updateEdges = allEdges.map(function(edge) {
                if (edge.from === selectedNode || edge.to === selectedNode) {
                    edge.color = {opacity: 1};
                } else {
                    edge.color = {opacity: 0.05};
                }
                return edge;
            });
            edges.update(updateEdges);
        }
    });

    network.on("deselectNode", function (params) {
        var allNodes = nodes.get();
        var allEdges = edges.get();
        
        var updateNodes = allNodes.map(function(node) {
            node.color = {opacity: 1};
            node.font = {color: "white", opacity: 1};
            return node;
        });
        nodes.update(updateNodes);
        
        var updateEdges = allEdges.map(function(edge) {
            edge.color = {opacity: 1};
            return edge;
        });
        edges.update(updateEdges);
    });

    // 드래그 종료 시 물리 연산 멈춤 (노드 고정)
    network.on("dragEnd", function (params) {
        if (params.nodes.length > 0) {
            var nodeId = params.nodes[0];
            nodes.update({id: nodeId, fixed: {x: true, y: true}});
        }
    });
    
    // 초기 안정화 후 물리 연산 중지
    network.once("stabilizationIterationsDone", function() {
        network.setOptions({ physics: false });
        console.log("Stabilization done, physics disabled.");
    });
    </script>
    """
    
    # </body> 태그 직전에 삽입
    new_content = content.replace("</body>", highlight_js + "</body>")
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"✅ 고도화된 시각화 완료: {OUTPUT_HTML}")

if __name__ == "__main__":
    generate_visualization()
