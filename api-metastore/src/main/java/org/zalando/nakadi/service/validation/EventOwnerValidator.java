package org.zalando.nakadi.service.validation;

import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.service.auth.AuthorizationResourceMapping;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class EventOwnerValidator {
    private static final Map<String, List<String>> ALLOWED_EOS_NAMES_BY_ASPD_DATA_CLASSIFICATION = Map.of(
            "none", Collections.emptyList(),
            // TODO: the below two data classifications might support merchant_id event-level discriminator
            //  in the future.
            "aspd", Collections.emptyList(),
            "mcf-aspd", List.of("retailer_id"));


    // TODO: taking into account that this is coupled with authorization,
    //  this should be encapsulated in a relevant component.
    public static void validateEventOwnerSelector(final EventTypeBase eventType) {
        checkState(eventType.getAnnotations() != null, "annotations map must be preset on the event type");
        final String dataClassification = eventType.getAnnotations()
                .get(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION);
        final EventOwnerSelector selector = eventType.getEventOwnerSelector();
        if (selector != null) {
            if (selector.getType() == EventOwnerSelector.Type.METADATA && selector.getValue() != null) {
                throw new InvalidEventTypeException(
                        "event_owner_selector specifying value for type 'metadata' is not supported");
            }
        }
        if (dataClassification == null) {
            // TODO: this is for backwards compatibility. Although we already could add some restrictions.
            return;
        }

        final var allowedSelectors = ALLOWED_EOS_NAMES_BY_ASPD_DATA_CLASSIFICATION.get(dataClassification);
        checkState(allowedSelectors != null, "not implemented (case: " + dataClassification + ")");

        if (selector != null && !allowedSelectors.contains(selector.getName())) {
            final String errorMessage;
            if (allowedSelectors.isEmpty()) {
                errorMessage = String.format(
                        "\"%s\" data classification with event_owner_selector is not allowed",
                        dataClassification);
            } else {
                errorMessage = String.format(
                        "\"%s\" data classification is compatible with event_owner_selector.name in {%s} set only",
                        dataClassification, String.join(", ", allowedSelectors));
            }
            throw new InvalidEventTypeException(errorMessage);
        }
    }

}
