from unittest import TestCase

from tc_analyzer_lib.algorithms.neo4j_analysis.closeness_centrality import (
    ClosenessCentrality,
)
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class TestClosenessCentralityWithMutualTies(TestCase):
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
            MERGE (b) -[r2:{interacted_with} {{weight: 1, date: {yesterday}}}]->(a)

            MERGE (a) -[r3:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (c) -[r4:{interacted_with} {{weight: 3, date: {yesterday}}}]->(a)

            MERGE (b) -[r5:{interacted_with} {{weight: 2, date: {yesterday}}}]->(c)
            MERGE (g)-[:HAVE_METRICS {{date: {yesterday}}}]->(g)

            SET r.platformId = '{platform_id}'
            SET r2.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            SET r5.platformId = '{platform_id}'
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

        self.assertEqual(len(results), 3)

        for _, row in results.iterrows():
            self.assertEqual(row["date"], yesterday)

            if row["userid"] == "a":
                self.assertEqual(row["closenessScore"], 1)
            elif row["userid"] == "b":
                self.assertAlmostEqual(row["closenessScore"], 2 / 3)
            elif row["userid"] == "c":
                self.assertAlmostEqual(row["closenessScore"], 2 / 3)
            else:
                raise ValueError("Never should reach here!")

    def test_three_vertices_two_dates_from_start_false(self):
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
            SET a.id = "a"
            SET b.id = "b"
            SET c.id = "c"
            MERGE (a) -[r:{interacted_with} {{weight: 1, date: {today}}}]->(b)
            MERGE (b) -[r2:{interacted_with} {{weight: 1, date: {today}}}]->(a)

            MERGE (a) -[r3:{interacted_with} {{weight: 1, date: {yesterday}}}]->(b)
            MERGE (b) -[r4:{interacted_with} {{weight: 2, date: {yesterday}}}]->(a)

            MERGE (a) -[r5:{interacted_with} {{weight: 3, date: {today}}}]->(c)
            MERGE (c) -[r6:{interacted_with} {{weight: 3, date: {today}}}]->(a)

            MERGE (a) -[r7:{interacted_with} {{weight: 3, date: {yesterday}}}]->(c)
            MERGE (c) -[r8:{interacted_with} {{weight: 3, date: {yesterday}}}]->(a)

            MERGE (b) -[r9:{interacted_with} {{weight: 2, date: {today}}}]->(c)
            MERGE (g)-[:HAVE_METRICS {{date: {today}}}]->(g)

            // random scores
            MERGE (a)-[:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}, closenessCentrality: 0.1}}]->(a)
            MERGE (b)-[:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}, closenessCentrality: 0.1}}]->(b)
            MERGE (c)-[:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}, closenessCentrality: 0.1}}]->(c)

            SET r.platformId = '{platform_id}'
            SET r2.platformId = '{platform_id}'
            SET r3.platformId = '{platform_id}'
            SET r4.platformId = '{platform_id}'
            SET r5.platformId = '{platform_id}'
            SET r6.platformId = '{platform_id}'
            SET r7.platformId = '{platform_id}'
            SET r8.platformId = '{platform_id}'
            SET r9.platformId = '{platform_id}'
            """
        )

        centrality = ClosenessCentrality(platform_id, self.graph_schema)
        centrality.compute(from_start=False)

        yesterday_results = self.neo4j_ops.gds.run_cypher(
            f"""
            MATCH (user:{self.graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {yesterday}}}]->(user)
            RETURN user.id as userid, r.date as date, r.closenessCentrality as closenessScore
            """
        )
        self.assertEqual(len(yesterday_results), 3)

        # the yesterday should not be computed so the
        # scores should be the random scores we assigned
        for _, row in yesterday_results.iterrows():
            self.assertEqual(row["date"], yesterday)

            if row["userid"] == "a":
                self.assertEqual(row["closenessScore"], 0.1)
            elif row["userid"] == "b":
                self.assertAlmostEqual(row["closenessScore"], 0.1)
            elif row["userid"] == "c":
                self.assertAlmostEqual(row["closenessScore"], 0.1)
            else:
                raise ValueError("Never should reach here!")

        today_results = self.neo4j_ops.gds.run_cypher(
            f"""
            MATCH (user:{self.graph_schema.user_label})-[r:HAVE_METRICS {{platformId: '{platform_id}', date: {today}}}]->(user)
            RETURN user.id as userid, r.date as date, r.closenessCentrality as closenessScore
            """
        )

        self.assertEqual(len(today_results), 3)
        for _, row in today_results.iterrows():
            self.assertEqual(row["date"], today)

            if row["userid"] == "a":
                self.assertEqual(row["closenessScore"], 1)
            elif row["userid"] == "b":
                self.assertAlmostEqual(row["closenessScore"], 2 / 3)
            elif row["userid"] == "c":
                self.assertAlmostEqual(row["closenessScore"], 2 / 3)
            else:
                raise ValueError("Never should reach here!")
