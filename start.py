"""通过 C2CCollect 聚合启动 multimedia_lib 节点。"""

import argparse
import logging

from adp_operator import serve
from adp_operator.collect import C2CCollect
from adp_operator.operators.adp_redis import ADPRedis
from adp_operator.operators.c2c_http_gateway import C2CHttpGateway
from adp_operator.operators.c2c_panel_node import C2CPanelNode

from lib.async_task_node import AsyncTaskNode
from lib.m3u8Download import m3u8Download
from lib.proxy_node import ProxyNode
from lib.video_play import VideoPlay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-id", default="MultimediaCollect")
    parser.add_argument("--network")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )

    multimedia = C2CCollect(
        ProxyNode,
        m3u8Download,
        AsyncTaskNode,
        VideoPlay,
        node_id=args.node_id,
        network=args.network,
    )
    gateway_panel = C2CCollect(
        C2CHttpGateway,
        C2CPanelNode,
        node_id="GatewayPanel",
        network=args.network,
    )
    collects = (multimedia, gateway_panel)

    try:
        for collect in collects:
            collect.start(blocking=False)
        redis_args = []
        if args.network is not None:
            redis_args.extend(["--network", args.network])
        serve(ADPRedis, redis_args)
    finally:
        for collect in reversed(collects):
            collect.stop()


if __name__ == "__main__":
    main()
