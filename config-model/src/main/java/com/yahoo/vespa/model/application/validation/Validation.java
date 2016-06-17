// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.model.application.validation;

import com.yahoo.config.application.api.DeployLogger;
import com.yahoo.config.model.api.ConfigChangeAction;
import com.yahoo.config.model.api.Model;
import com.yahoo.config.model.deploy.DeployState;
import com.yahoo.vespa.model.VespaModel;
import com.yahoo.vespa.model.application.validation.change.ChangeValidator;
import com.yahoo.vespa.model.application.validation.change.ClusterSizeReductionValidator;
import com.yahoo.vespa.model.application.validation.change.ConfigValueChangeValidator;
import com.yahoo.vespa.model.application.validation.change.ContainerRestartValidator;
import com.yahoo.vespa.model.application.validation.change.ContentClusterRemovalValidator;
import com.yahoo.vespa.model.application.validation.change.IndexedSearchClusterChangeValidator;
import com.yahoo.vespa.model.application.validation.change.IndexingModeChangeValidator;
import com.yahoo.vespa.model.application.validation.change.StartupCommandChangeValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

/**
 * Executor of validators. This defines the right order of validator execution.
 * Validators that must be run after search cluster search definition deriving are
 * defined in PostSdValidation.
 *
 * @author hmusum
 * @since 2010-01-29
 */
public class Validation {

    /** Validate everything */
    public static List<ConfigChangeAction> validate(VespaModel model, boolean force, DeployState deployState) {
        return validate(model, true, force, deployState);
    }

    /**
     * Validate with optional checking of routing, which cannot always be valid in unit tests
     *
     * @return a list of required changes needed to make this configuration live
     */
    public static List<ConfigChangeAction> validate(VespaModel model, boolean checkRouting, boolean force, DeployState deployState) {
        if (checkRouting) {
            new RoutingValidator().validate(model, deployState);
            new RoutingSelectorValidator().validate(model, deployState);
        }
        new ComponentValidator().validate(model, deployState);
        new SearchDataTypeValidator().validate(model, deployState);
        new StreamingValidator().validate(model, deployState);
        new RankSetupValidator(force).validate(model, deployState);
        new NoPrefixForIndexes().validate(model, deployState);
        new DeploymentFileValidator().validate(model, deployState);

        Optional<Model> currentActiveModel = deployState.getPreviousModel();
        if (currentActiveModel.isPresent() && (currentActiveModel.get() instanceof VespaModel))
            return validateChanges((VespaModel)currentActiveModel.get(), model,
                                   deployState.validationOverrides(), deployState.getDeployLogger());
        else
            return new ArrayList<>();
    }

    private static List<ConfigChangeAction> validateChanges(VespaModel currentModel, VespaModel nextModel,
                                                            ValidationOverrides overrides, DeployLogger logger) {
        ChangeValidator[] validators = new ChangeValidator[] {
                new IndexingModeChangeValidator(),
                new IndexedSearchClusterChangeValidator(),
                new ConfigValueChangeValidator(logger),
                new StartupCommandChangeValidator(),
                new ContentClusterRemovalValidator(),
                new ClusterSizeReductionValidator(),
                new ContainerRestartValidator(),
        };
        return Arrays.stream(validators)
                .flatMap(v -> v.validate(currentModel, nextModel, overrides).stream())
                .collect(toList());
    }

}
