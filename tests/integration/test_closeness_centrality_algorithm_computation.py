from unittest import TestCase

from tc_analyzer_lib.algorithms.neo4j_analysis.closeness_centrality import (
    ClosenessCentrality,
)
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class TestClosenessCentrality(TestCase):
    def setUp(self) -> None:
        self.neo4j_ops = Neo4jOps.get_instance()
        # deleting all data
        self.neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    def tearDown(self) -> None:
        self.neo4j_ops.gds.run_cypher("MATCH (n) DETACH DELETE (n)")

    def test_available_date(self):
        """
        test the closeness centrality algorithm with some nodes connected
        """
        # timestamps
        today = 1689280200.0
        yesterday = 1689193800.0
        graph_schema = GraphSchema(platform="discord")
        platform_id = "5151515151515"

        user_label = graph_schema.user_label
        platform_label = graph_schema.platform_label
        interacted_with = graph_schema.interacted_with_rel
        is_member = graph_schema.member_relation

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
            MERGE (b) -[r2:{interacted_with} {{weight: 1, date: {yesterday}}}]->(a)
            MERGE (a) -[r3:{interacted_with} {{weight: 2, date: {today}}}]->(b)
            MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {today}}}]->(a)
            MERGE (a) -[r5:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (c) -[r6:{interacted_with} {{weight: 3, date: {yesterday}}}]->(a)
            MERGE (b) -[r7:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
            MERGE (c) -[r8:{interacted_with} {{weight: 2, date: {yesterday}}}]->(b)
            SET r.platformId = '{platform_id}'
            SET r2.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            SET r5.platformId = '{platform_id}'
            SET r6.platformId = '{platform_id}'
            SET r7.platformId = '{platform_id}'
            SET r8.platformId = '{platform_id}'
            """
        )
        louvain = ClosenessCentrality(platform_id, graph_schema)
        louvain.compute(from_start=False)

        # yesterday_results = self.neo4j_ops.gds.run_cypher(
        #     f"""
        #     MATCH (user:{graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}}}]->(user)
        #     RETURN r.date as date, r.closenessCentrality as closenessScore
        #     """
        # )
        # # yesterday three users were interacting
        # assert len(yesterday_results) == 3
        # assert yesterday_results["date"].iloc[0] == yesterday

        today_results = self.neo4j_ops.gds.run_cypher(
            f"""
            MATCH (user:{graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {today}}}]->(user)
            RETURN r.date as date, r.closenessCentrality as closenessScore
            """
        )
        # today just two users interacting
        assert len(today_results) == 2
        for i in range(2):
            assert today_results["date"].iloc[i] == today

    def test_more_available_data(self):
        """
        test the louvain algorithm with some more data available
        """
        # timestamps
        today = 1689280200.0
        yesterday = 1689193800.0
        graph_schema = GraphSchema(platform="discord")
        platform_id = "5151515151515"

        user_label = graph_schema.user_label
        platform_label = graph_schema.platform_label
        interacted_with = graph_schema.interacted_with_rel
        is_member = graph_schema.member_relation

        # creating some nodes with data
        self.neo4j_ops.gds.run_cypher(
            f"""
            CREATE (a:{user_label}) -[:{is_member}]->(g:{platform_label} {{id: '{platform_id}'}})
            CREATE (b:{user_label}) -[:{is_member}]->(g)
            CREATE (c:{user_label}) -[:{is_member}]->(g)
            CREATE (d:{user_label}) -[:{is_member}]->(g)
            CREATE (e:{user_label}) -[:{is_member}]->(g)
            SET a.id = "1000"
            SET b.id = "1001"
            SET c.id = "1002"
            SET d.id = "1003"
            SET e.id = "1004"
            
            // creating mutual ties
            MERGE (a) -[:{interacted_with} {{date: {yesterday}, weight: 1}}]->(b)
            MERGE (b) -[:{interacted_with} {{date: {yesterday}, weight: 1}}]->(a)

            MERGE (a) -[:{interacted_with} {{date: {yesterday}, weight: 3}}]->(d)
            MERGE (d) -[:{interacted_with} {{date: {yesterday}, weight: 3}}]->(a)

            MERGE (c) -[:{interacted_with} {{date: {yesterday}, weight: 2}}]->(b)
            MERGE (b) -[:{interacted_with} {{date: {yesterday}, weight: 2}}]->(c)

            MERGE (c) -[:{interacted_with} {{date: {yesterday}, weight: 2}}]->(d)
            MERGE (d) -[:{interacted_with} {{date: {yesterday}, weight: 2}}]->(c)
            
            MERGE (d) -[:{interacted_with} {{date: {yesterday}, weight: 1}}]->(b)
            MERGE (b) -[:{interacted_with} {{date: {yesterday}, weight: 1}}]->(d)

            MERGE (a) -[:{interacted_with} {{date: {today}, weight: 2}}]->(b)
            MERGE (b) -[:{interacted_with} {{date: {today}, weight: 2}}]->(a)

            MERGE (c) -[:{interacted_with} {{date: {today}, weight: 1}}]->(b)
            MERGE (b) -[:{interacted_with} {{date: {today}, weight: 1}}]->(c)
            
            MERGE (c) -[:{interacted_with} {{date: {today}, weight: 2}}]->(a)
            MERGE (a) -[:{interacted_with} {{date: {today}, weight: 2}}]->(c)

            MERGE (d) -[:{interacted_with} {{date: {today}, weight: 1}}]->(c)
            MERGE (c) -[:{interacted_with} {{date: {today}, weight: 1}}]->(d)

            MERGE (b) -[:{interacted_with} {{date: {today}, weight: 2}}]->(d)
            MERGE (d) -[:{interacted_with} {{date: {today}, weight: 2}}]->(b)

            MERGE (d) -[:{interacted_with} {{date: {today}, weight: 1}}]->(c)
            MERGE (c) -[:{interacted_with} {{date: {today}, weight: 1}}]->(d)

            MERGE (e) -[:{interacted_with} {{date: {today}, weight: 3}}]->(b)
            MERGE (b) -[:{interacted_with} {{date: {today}, weight: 3}}]->(e)

            MERGE (:{user_label})-[r:{interacted_with}]->(:{user_label})
            SET r.platformId = '{platform_id}'
            """
        )

        louvain = ClosenessCentrality(platform_id, graph_schema)

        louvain.compute(from_start=False)

        # yesterday_results = self.neo4j_ops.gds.run_cypher(
        #     f"""
        #     MATCH (user:{graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}}}]->(user)
        #     RETURN r.date as date, r.closenessCentrality as closenessScore
        #     """
        # )
        # # yesterday 4 users were interacting
        # assert len(yesterday_results) == 4
        # assert yesterday_results["date"].iloc[0] == yesterday

        today_results = self.neo4j_ops.gds.run_cypher(
            f"""
            MATCH (user:{graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {today}}}]->(user)
            RETURN r.date as date, r.closenessCentrality as closenessScore
            """
        )
        # today 5 users interacting
        assert len(today_results) == 5
        for i in range(5):
            assert today_results["date"].iloc[i] == today
