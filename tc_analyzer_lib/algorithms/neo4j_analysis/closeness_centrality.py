import logging
from uuid import uuid1

from tc_analyzer_lib.algorithms.neo4j_analysis.utils import ProjectionUtils
from tc_analyzer_lib.schemas import GraphSchema
from tc_neo4j_lib.neo4j_ops import Neo4jOps


class ClosenessCentrality:
    def __init__(self, platform_id: str, graph_schema: GraphSchema) -> None:
        """
        Closeness centrality algorithm wrapper to compute
        """
        self.neo4j_ops = Neo4jOps.get_instance()
        self.platform_id = platform_id
        self.graph_schema = graph_schema

        self.projection_utils = ProjectionUtils(
            platform_id=platform_id, graph_schema=graph_schema
        )
        self.log_prefix = f"PLATFORMID: {platform_id} "

    def compute(self, from_start: bool = False) -> None:
        """
        compute the closeness centrality score for a platform

        Parameters
        ------------
        from_start : bool
            whether to compute the metric from the first day or not
            if True, then would compute from start
            default is False
        """

        computable_dates = self.projection_utils.get_dates()

        # compute for each date
        to_compute: set[float]
        if from_start:
            to_compute = computable_dates
        else:
            computed_dates = self.get_computed_dates()
            to_compute = computable_dates - computed_dates

        for date in to_compute:
            try:
                self.closeness_computation_wrapper(date)
            except Exception as exp:
                logging.error(
                    f"Exception: {self.log_prefix}Closeness Centrality "
                    f" computation for date: {date}, exp: {exp}"
                )

    def closeness_computation_wrapper(self, date: float) -> None:
        """
        a wrapper for closeness centrality computation process
        we're doing the projection here and computing on that,
        then we'll drop the pojection

        Parameters:
        ------------
        date : float
            timestamp of the relation
        """
        graph_projected_name = f"GraphClosenessCentrality_{uuid1()}"

        user_label = self.graph_schema.user_label
        relation_label = self.graph_schema.interacted_with_rel

        self.projection_utils.project_temp_graph(
            graph_name=graph_projected_name,
            weighted=False,
            date=date,
            relation_direction="NATURAL",
            projection_query=f"""
            MATCH (a:{user_label})-[
                r:{relation_label} {{platformId: '{self.platform_id}', date: {date}}}
            ]->(b:{user_label}),
            (b)-[:{relation_label} {{platformId: '{self.platform_id}', date: {date}}}]->(a)
            """,
        )

        # get the results as pandas dataframe
        self.compute_graph_closeness(date=date, graph_name=graph_projected_name)

        # dropping the computed date
        _ = self.neo4j_ops.gds.run_cypher(
            """
            CALL gds.graph.drop($graph_projected_name)
            """,
            {
                "graph_projected_name": graph_projected_name,
            },
        )

    def get_computed_dates(self) -> set[float]:
        """
        get closeness centrality computed dates

        Returns:
        ----------
        computed_dates : set[float]
            the computation dates
        """
        # getting the dates computed before
        user_label = self.graph_schema.user_label
        query = f"""
            MATCH (user: {user_label})
                -[r:HAVE_METRICS {{platformId: $platform_id}}]->(user)
            WHERE r.closenessCentrality IS NOT NULL
            RETURN r.date as computed_dates
            """
        computed_dates = self.projection_utils.get_computed_dates(
            query, platform_id=self.platform_id
        )

        return computed_dates

    def compute_graph_closeness(self, date: float, graph_name: str) -> None:
        """
        compute closeness centrality for the projected graph and
        save the results back into db

        Parameters:
        ------------
        date : float
            timestamp of the relation
        graph_name : str
            the operation would be done on the graph
        """
        try:
            user_label = self.graph_schema.user_label

            _ = self.neo4j_ops.gds.run_cypher(
                f"""
                CALL gds.closeness.stream($graph_name)
                YIELD nodeId, score
                WITH gds.util.asNode(nodeId).id AS user_id, score
                MATCH (user:{user_label} {{id: user_id}})
                MERGE (user)
                    -[r:HAVE_METRICS {{date: $date, platformId: $platform_id}}]
                ->(user)
                SET r.closenessCentrality = score
                """,
                {
                    "graph_name": graph_name,
                    "date": date,
                    "platform_id": self.platform_id,
                },
            )
        except Exception as exp:
            logging.error(
                f"{self.log_prefix} Error in computing "
                f"closeness centrality algorithm, {exp}"
            )
