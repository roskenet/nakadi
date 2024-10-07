package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class CursorsServiceTest {

    private AuthorizationValidator authorizationValidator;
    private CursorsService service;

    @Before
    public void setup() {
        authorizationValidator = mock(AuthorizationValidator.class);
        service = new CursorsService(mock(SubscriptionDbRepository.class), mock(SubscriptionCache.class), null,
                null, null, null, null, authorizationValidator, null);
    }

    @Test(expected = AccessDeniedException.class)
    public void whenResetCursorsThenAdminAccessChecked() {
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionAdmin(any());
        service.resetCursors("test", Collections.emptyList());
    }

    @Test(expected = AccessDeniedException.class)
    public void whenCommitCursorsAccessDenied() {
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN,
                new ResourceImpl<Subscription>("", ResourceImpl.SUBSCRIPTION_RESOURCE, null, null)))
                .when(authorizationValidator).authorizeSubscriptionCommit(any());
        service.commitCursors("test", "test", Collections.emptyList());
    }

    @Test
    public void testCursorsForResetAffectAssignedPartitions() {
        final Partition p0 = new Partition("et", "0", "123", null, Partition.State.ASSIGNED);
        final Partition p1 = new Partition("et", "1", "123", "abc", Partition.State.REASSIGNING);
        final Partition p2 = new Partition("et", "2", null, null, Partition.State.UNASSIGNED);
        final Partition[] partitions = new Partition[]{ p0, p1, p2 };

        final SubscriptionCursorWithoutToken c0 = new SubscriptionCursorWithoutToken("et", "0", "0001");
        final SubscriptionCursorWithoutToken c1 = new SubscriptionCursorWithoutToken("et", "1", "0001");
        final SubscriptionCursorWithoutToken c2 = new SubscriptionCursorWithoutToken("et", "2", "0001");
        final SubscriptionCursorWithoutToken otherC0 = new SubscriptionCursorWithoutToken("other-et", "0", "0001");

        Assert.assertTrue(CursorsService.cursorsResetAffectsAssignedPartitions(partitions, List.of(c0)));
        Assert.assertTrue(CursorsService.cursorsResetAffectsAssignedPartitions(partitions, List.of(c1)));
        Assert.assertFalse(CursorsService.cursorsResetAffectsAssignedPartitions(partitions, List.of(c2)));

        Assert.assertTrue(CursorsService.cursorsResetAffectsAssignedPartitions(partitions, List.of(c0, c2)));

        Assert.assertFalse(CursorsService.cursorsResetAffectsAssignedPartitions(partitions, List.of(otherC0)));
    }
}
