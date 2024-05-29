// package it.agilelab.witboost.provisioning.databricks.client;
//
// import static io.vavr.control.Either.left;
// import static io.vavr.control.Either.right;
//
// import com.databricks.sdk.WorkspaceClient;
// import com.databricks.sdk.core.DatabricksError;
// import com.databricks.sdk.service.compute.ClusterDetails;
// import com.databricks.sdk.service.compute.CreateCluster;
// import com.databricks.sdk.service.compute.CreateClusterResponse;
// import com.databricks.sdk.service.compute.State;
// import io.vavr.control.Either;
// import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
// import it.agilelab.witboost.provisioning.databricks.common.Problem;
// import java.util.Collections;
// import java.util.logging.Logger;
//
/// **
// * NOTE: class implemented for future use
// * Class to manage Databricks clusters.
// */
// public class ClusterManager {
//
//    static Logger logger = Logger.getLogger(ClusterManager.class.getName());
//
//    /**
//     * Creates a new Databricks cluster.
//     *
//     * @param workspaceClient        The WorkspaceClient instance.
//     * @param clusterName            The name of the cluster to be created.
//     * @param sparkVersion           The Spark version for the cluster.
//     * @param nodeTypeId             The node type ID for the cluster.
//     * @param autoTerminationMinutes The auto-termination time for the cluster.
//     * @param numWorkers             The number of workers for the cluster.
//     * @return                  Either a list of failed operations if an exception occurs during deletion, or the
// cluster ID if successful.
//     */
//    public Either<FailedOperation, String> createCluster(
//            WorkspaceClient workspaceClient,
//            String clusterName,
//            String sparkVersion,
//            String nodeTypeId,
//            Long autoTerminationMinutes,
//            Long numWorkers) {
//
//        try {
//
//            logger.info("Creating new cluster...");
//
//            CreateClusterResponse c = workspaceClient
//                    .clusters()
//                    .create(new CreateCluster()
//                            .setClusterName(clusterName)
//                            .setSparkVersion(sparkVersion)
//                            .setNodeTypeId(nodeTypeId)
//                            .setAutoterminationMinutes(autoTerminationMinutes)
//                            .setNumWorkers(numWorkers))
//                    .getResponse();
//
//            logger.info("View the cluster at " + workspaceClient.config().getHost()
//                    + "#setting/clusters/"
//                    + c.getClusterId()
//                    + "/configuration\n");
//
//            return right(c.getClusterId());
//
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
//        }
//    }
//
//    /**
//     * Starts a Databricks cluster.
//     *
//     * @param workspaceClient The WorkspaceClient instance.
//     * @param clusterID       The ID of the cluster to start
//     * @return                Either a Boolean indicating success or a list of failed operations if an exception
// occurs during cluster start.
//     */
//    public Either<FailedOperation, Boolean> startCluster(WorkspaceClient workspaceClient, String clusterID) {
//
//        logger.info("Starting the cluster...");
//        try {
//            ClusterDetails c = workspaceClient.clusters().get(clusterID);
//            if (c.getState() == State.RUNNING) {
//                logger.info("Cluster is running at " + workspaceClient.config().getHost()
//                        + "#setting/clusters/"
//                        + clusterID
//                        + "/configuration\n");
//
//                return right(true);
//            } else if (c.getState() != State.TERMINATED) {
//                return left(new FailedOperation(Collections.singletonList(
//                        new Problem("Can start clusters only in 'TERMINATED' state," + " the cluster is now in "
//                                + c.getState()
//                                + " state"))));
//            } else {
//                workspaceClient.clusters().start(clusterID).get();
//                logger.info("Ok");
//                return right(true);
//            }
//
//        } catch (DatabricksError e) {
//            logger.info("Error starting the cluster with ID " + clusterID);
//            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
//        }
//    }
//
//    /**
//     * Stops a Databricks cluster.
//     *
//     * @param workspaceClient The WorkspaceClient instance.
//     * @param clusterID       The ID of the cluster to stop.
//     * @return                  Either a list of failed operations if an exception occurs during deletion, or void if
// successful.
//     */
//    public Either<FailedOperation, Void> stopCluster(WorkspaceClient workspaceClient, String clusterID) {
//        logger.info("Stopping the cluster...");
//        try {
//            workspaceClient.clusters().delete(clusterID).get();
//            logger.info("Cluster stopped");
//            return right(null);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
//        }
//    }
//
//    /**
//     * Permanently delete a Databricks cluster.
//     * This cluster is terminated and resources are asynchronously removed.
//     *
//     * @param workspaceClient The WorkspaceClient instance.
//     * @param clusterID       The ID of the cluster to stop.
//     * @return                  Either a list of failed operations if an exception occurs during deletion, or void if
// successful.
//     */
//    public Either<FailedOperation, Void> deleteCluster(WorkspaceClient workspaceClient, String clusterID) {
//
//        try {
//            workspaceClient.clusters().permanentDelete(clusterID);
//            logger.info("Cluster deleted");
//            return Either.right(null);
//        } catch (Exception e) {
//            logger.severe(e.getMessage());
//            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
//        }
//    }
// }
