package org.zalando.nakadi.cache;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class ChangeSetTest {

    public static class TestCase {
        private final String name;
        private final Change[] initialChanges;
        private final Change[] changeSet;
        private final String[] expectedUpdatedEts;
        private final String[] expectedDeleteChangeIds;
        private final long ttl;

        public TestCase(
                final String name,
                final Change[] initialChanges,
                final Change[] changeSet,
                final String[] expectedUpdatedEts,
                final String[] expectedDeleteChangeIds,
                final long ttl) {
            this.name = name;
            this.initialChanges = initialChanges;
            this.changeSet = changeSet;
            this.expectedUpdatedEts = expectedUpdatedEts;
            this.expectedDeleteChangeIds = expectedDeleteChangeIds;
            this.ttl = ttl;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Parameterized.Parameter
    public TestCase valueToTest;

    @Parameterized.Parameters
    public static Iterable<TestCase> testCases() {
        final Date date1 = new Date(System.currentTimeMillis() - 4000);
        final Date date2 = new Date();
        System.out.println("date1: " + date1 + " and date2: " + date2);
        final Change change1et1 = new Change("change1", "et1", date1);
        final Change change2et1 = new Change("change2", "et1", date2);
        final Change change3et2 = new Change("change3", "et2", date1);
        final Change change4et2 = new Change("change4", "et2", date2);

        return Arrays.asList(
                new TestCase("emptyLists", new Change[]{}, new Change[]{}, new String[]{}, new String[]{}, 5000),
                new TestCase("notificationAddedBySomeone",
                        new Change[]{}, new Change[]{change1et1}, new String[]{"et1"}, new String[]{}, 7000),
                new TestCase("notificationRemovedBySomeone",
                        new Change[]{change1et1}, new Change[]{}, new String[]{}, new String[]{}, 7000),
                new TestCase("secondNotificationAddedBySomeone",
                        new Change[]{change1et1}, new Change[]{change1et1, change2et1},
                        new String[]{"et1"}, new String[]{"change1"}, 7000),
                new TestCase("2EventTypesUpdated",
                        new Change[]{}, new Change[]{change1et1, change3et2},
                        new String[]{"et1", "et2"}, new String[]{}, 7000),
                new TestCase("OldChangesAreRemovedBecauseTheyAreOld",
                        new Change[]{}, new Change[]{change1et1},
                        new String[]{"et1"}, new String[]{"change1"}, 2000),
                new TestCase("OnlyLatestChangeIsKeptInZk",
                        new Change[]{}, new Change[]{change1et1, change2et1, change3et2, change4et2},
                        new String[]{"et1", "et2"}, new String[]{"change1", "change3"}, 7000),
                new TestCase("NoChangesInCaseIfDataTheSame",
                        new Change[]{change1et1, change3et2}, new Change[]{change3et2, change1et1},
                        new String[]{}, new String[]{}, 7000)
        );
    }

    @Test
    public void performSomeTest() {
        final ChangeSet changeSet = new ChangeSet();
        changeSet.apply(Arrays.asList(valueToTest.initialChanges));
        final List<Change> newChanges = Arrays.asList(valueToTest.changeSet);

        final Collection<String> changedEventTypes = changeSet.getUpdatedEventTypes(newChanges);
        System.out.println("changedEventTypes: " + changedEventTypes);
        System.out.println("valueToTest ets: " + Arrays.asList(valueToTest.expectedUpdatedEts));
        Assert.assertEquals(valueToTest.expectedUpdatedEts.length, changedEventTypes.size());
        Stream.of(valueToTest.expectedUpdatedEts).forEach(v -> Assert.assertTrue(changedEventTypes.contains(v)));

        final Collection<Change> changesToDelete = changeSet.getChangesToRemove(newChanges, valueToTest.ttl);
        final Set<String> realChangesToDelete = changesToDelete.stream().map(Change::getId).collect(Collectors.toSet());
        System.out.println("realChangesToDelete: " + realChangesToDelete);
        System.out.println("valueToTest deletes: " + Arrays.asList(valueToTest.expectedDeleteChangeIds));
        Assert.assertEquals(valueToTest.expectedDeleteChangeIds.length, realChangesToDelete.size());
        Stream.of(valueToTest.expectedDeleteChangeIds).forEach(d -> Assert.assertTrue(realChangesToDelete.contains(d)));
    }
}