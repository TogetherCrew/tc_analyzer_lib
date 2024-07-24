from unittest import TestCase

from tc_analyzer_lib.algorithms.neo4j_analysis.closeness_centrality import (
    ClosenessCentrality,
)
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class TestClosenessCentralityGetComputedDates(TestCase):
    def setUp(self) -> None:
        self.neo4j_ops = Neo4jOps.get_instance()
        # deleting all data
        self.neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")
        self.graph_schema = GraphSchema(platform="discord")

    def tearDown(self) -> None:
        self.neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    def test_empty_data(self):
        """
        test with empty data for getting the computed dates
        """
        # timestamps
        today = 1689280200.0
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
            SET a.id = "1000"
            SET b.id = "1001"
            SET c.id = "1002"
            MERGE (a) -[r:{interacted_with} {{weight: 1, date: {yesterday}}}]->(b)
            MERGE (a) -[r2:{interacted_with} {{weight: 2, date: {today}}}]->(b)
            MERGE (a) -[r3:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
            SET r.platformId = '{platform_id}'
            SET r2.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            """
        )
        closeness_centrality = ClosenessCentrality(platform_id, self.graph_schema)

        computed_dates = closeness_centrality.get_computed_dates()

        self.assertEqual(computed_dates, set())

    def test_empty_data_with_have_metrics_relation(self):
        """
        test with empty data for getting the computed dates
        """
        # timestamps
        today = 1689280200.0
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
            SET a.id = "1000"
            SET b.id = "1001"
            SET c.id = "1002"
            MERGE (a) -[r:{interacted_with} {{weight: 1, date: {yesterday}}}]->(b)
            MERGE (a) -[r2:{interacted_with} {{weight: 2, date: {today}}}]->(b)
            MERGE (a) -[r3:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
            MERGE (g)-[:HAVE_METRICS {{date: {yesterday}}}]->(g)

            MERGE (b)-[:HAVE_METRICS {{date: {yesterday}, platformId: '{platform_id}', closenessCentrality: 0.3}}]->(b)
            SET r.platformId = '{platform_id}'
            SET r2.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            """
        )
        closeness_centrality = ClosenessCentrality(platform_id, self.graph_schema)
        computed_dates = closeness_centrality.get_computed_dates()

        print("computed_dates", computed_dates)
        self.assertEqual(computed_dates, {yesterday})

    def test_two_dates(self):
        """
        test with empty data for getting the computed dates
        """
        # timestamps
        today = 1689280200.0
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
            SET a.id = "1000"
            SET b.id = "1001"
            SET c.id = "1002"
            MERGE (a) -[r:{interacted_with} {{weight: 1, date: {yesterday}}}]->(b)
            MERGE (a) -[r2:{interacted_with} {{weight: 2, date: {today}}}]->(b)
            MERGE (a) -[r3:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
            MERGE (g)-[:HAVE_METRICS {{date: {yesterday}, louvainModularityScore: 0.0}}]->(g)

            MERGE (a)-[:HAVE_METRICS {{date: {yesterday}, platformId: '{platform_id}', closenessCentrality: 0.2}}]->(a)
            MERGE (c)-[:HAVE_METRICS {{date: {yesterday}, platformId: '{platform_id}', closenessCentrality: 0.3}}]->(c)
            MERGE (b)-[:HAVE_METRICS {{date: {yesterday}, platformId: '{platform_id}', closenessCentrality: 0.5}}]->(b)

            MERGE (a)-[:HAVE_METRICS {{date: {today}, platformId: '{platform_id}', closenessCentrality: 0.5}}]->(a)
            MERGE (b)-[:HAVE_METRICS {{date: {today}, platformId: '{platform_id}', closenessCentrality: 0.5}}]->(b)

            SET r.platformId = '{platform_id}'
            SET r2.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            """
        )
        closeness_centrality = ClosenessCentrality(platform_id, self.graph_schema)
        computed_dates = closeness_centrality.get_computed_dates()

        self.assertEqual(computed_dates, {yesterday, today})
