package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

/**
 * NOTE: class implemented for future use
 * Class that manages operations on notebooks within the Databricks Workspace.
 */
public class NotebookManager {
    static Logger logger = Logger.getLogger(NotebookManager.class.getName());

    /**
     * Exports a notebook from the Workspace.
     *
     * @param workspaceClient The Databricks Workspace client.
     * @param sourcePath      The path of the notebook to export.
     * @param destinationPath The destination path for the export.
     * @param exportFormat    The desired export format.
     * @return                Either a success or a failure indication.
     */
    public Either<FailedOperation, Void> exportNotebook(
            WorkspaceClient workspaceClient, String sourcePath, String destinationPath, ExportFormat exportFormat) {

        try {

            ExportRequest exportRequest =
                    new ExportRequest().setPath(sourcePath).setFormat(exportFormat);

            ExportResponse notebook = workspaceClient.workspace().export(exportRequest);

            try (FileOutputStream writer = new FileOutputStream(destinationPath)) {
                byte[] decodedBytes = Base64.getDecoder().decode(notebook.getContent());
                writer.write(decodedBytes);
                logger.log(Level.INFO, "Notebook exported successfully!");
                return right(null);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "An error occurred while saving notebook.");
                return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    /**
     * Imports a notebook into the Workspace.
     *
     * @param workspaceClient The Databricks Workspace client.
     * @param sourcePath      The source path of the notebook to import.
     * @param destinationPath The destination path in the Workspace.
     * @param importFormat    The format of the notebook to import.
     * @param language        The language of the notebook.
     * @param setOverwrite    Flag indicating whether to overwrite existing notebook.
     * @return                Either a success or a failure indication.
     */
    public Either<FailedOperation, Void> importNotebook(
            WorkspaceClient workspaceClient,
            String sourcePath,
            String destinationPath,
            ImportFormat importFormat,
            Language language,
            Boolean setOverwrite) {

        try {
            File file = new File(sourcePath);
            String encoded = Base64.getEncoder().encodeToString(FileUtils.readFileToByteArray(file));

            Import importRequest = new Import()
                    .setPath(destinationPath)
                    .setFormat(importFormat)
                    .setLanguage(language)
                    .setOverwrite(setOverwrite)
                    .setContent(encoded);

            workspaceClient.workspace().importContent(importRequest);
            logger.log(Level.INFO, "Notebook imported successfully!");

            return right(null);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}
