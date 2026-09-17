import io
import json
import unittest
from unittest.mock import patch

from ray_nodes import RayNodeClient, parse_node_response


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class RayNodeClientTests(unittest.TestCase):
    def test_parses_compact_node_metrics(self):
        snapshot = parse_node_response(
            {
                "result": True,
                "data": {
                    "summary": [
                        {
                            "hostname": "head",
                            "ip": "172.18.0.2",
                            "cpu": 25,
                            "mem": [1024, 768, 25, 256],
                            "disk": {"/": {"total": 2048, "used": 512, "percent": 25}},
                            "raylet": {
                                "nodeId": "node-1",
                                "state": "ALIVE",
                                "isHeadNode": True,
                                "storeStats": {
                                    "objectStoreBytesUsed": "128",
                                    "objectStoreBytesAvail": "1024",
                                },
                            },
                        }
                    ]
                },
            }
        )

        self.assertEqual(snapshot.alive, 1)
        self.assertEqual(snapshot.nodes[0]["node_id"], "node-1")
        self.assertEqual(snapshot.nodes[0]["name"], "head")
        self.assertEqual(snapshot.nodes[0]["node"], "head (Head)")
        self.assertEqual(snapshot.nodes[0]["memory_percent"], 25)
        self.assertEqual(snapshot.nodes[0]["object_store_percent"], 12.5)
        self.assertEqual(snapshot.nodes[0]["disk_percent"], 25)

    def test_rejects_failed_or_unexpected_responses(self):
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            parse_node_response({"result": False, "msg": "unavailable"})
        with self.assertRaisesRegex(TypeError, "unexpected"):
            parse_node_response({"result": True, "data": {}})

    @patch("ray_nodes.urlopen")
    def test_fetches_node_summary(self, urlopen):
        urlopen.return_value = Response(
            json.dumps({"result": True, "data": {"summary": []}}).encode()
        )

        snapshot = RayNodeClient("http://control-plane:8265/").fetch()

        request = urlopen.call_args.args[0]
        self.assertEqual(snapshot.nodes, [])
        self.assertEqual(
            request.full_url, "http://control-plane:8265/nodes?view=summary"
        )


if __name__ == "__main__":
    unittest.main()
