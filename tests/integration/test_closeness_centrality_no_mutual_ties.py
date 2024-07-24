from unittest import TestCase

from tc_analyzer_lib.algorithms.neo4j_analysis.closeness_centrality import (
    ClosenessCentrality,
)
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class TestClosenessCentralityNoMutualTies(TestCase):
    """
    all scores should be zero
    """

    def setUp(self) -> None:
        self.neo4j_ops = Neo4jOps.get_instance()
        # deleting all data
        self.neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")
        self.graph_schema = GraphSchema(platform="discord")

    def tearDown(self) -> None:
        self.neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    def test_empty_data(self):
        platform_id = "5151515151"
        centrality = ClosenessCentrality(platform_id, self.graph_schema)

        centrality.compute(from_start=True)

        results = self.neo4j_ops.gds.run_cypher("MATCH (n) RETURN (n)")

        self.assertEqual(len(results), 0)

    def test_three_vertices_one_date(self):
        # timestamps
        yesterday = 1689193800.0
        platform_id = "5151515151515"

        user_label = self.graph_schema.user_label
        platform_label = self.graph_schema.platform_label
        interacted_with = self.graph_schema.interacted_with_rel
        is_member = self.graph_schema.member_relation

        # creating some nodes with data
        self.neo4j_ops.gds.run_cypher(
            f"""
            CREATE (a:{user_label}) -[:{is_member}]->(g:{platform_label} {{id: '{platform_id}'}})
            CREATE (b:{user_label}) -[:{is_member}]->(g)
            CREATE (c:{user_label}) -[:{is_member}]->(g)
            SET a.id = "a"
            SET b.id = "b"
            SET c.id = "c"
            MERGE (a) -[r:{interacted_with} {{weight: 1, date: {yesterday}}}]->(b)
            MERGE (a) -[r3:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
            MERGE (g)-[:HAVE_METRICS {{date: {yesterday}}}]->(g)

            SET r.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            """
        )

        centrality = ClosenessCentrality(platform_id, self.graph_schema)
        centrality.compute(from_start=False)

        results = self.neo4j_ops.gds.run_cypher(
            f"""
            MATCH (user:{self.graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}}}]->(user)
            RETURN user.id as userid, r.date as date, r.closenessCentrality as closenessScore
            """
        )

        self.assertEqual(len(results), 0)
