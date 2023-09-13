package com.armquiro.aggregatortest;

import org.springframework.integration.aggregator.ReleaseStrategy;
import org.springframework.integration.store.MessageGroup;
import org.springframework.integration.util.UUIDConverter;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Component
public class ExceptionAwareReleaseStrategy implements ReleaseStrategy {

  private Set<String> groupsWithExceptions = new HashSet<>();

  // Mark a group as having an exception
  public void markGroupWithException(UUID groupId) {
    groupsWithExceptions.add(groupId.toString());
  }

  @Override
  public boolean canRelease(MessageGroup group) {
    // Check if the group has encountered an exception
    //return !groupsWithExceptions.contains(group.getGroupId()) || group.size() == 3;
    UUID groupIdUuid = UUIDConverter.getUUID(group.getGroupId().toString());
    System.out.println("###### looking for " + groupIdUuid.toString() + " and found: " + groupsWithExceptions.contains(groupIdUuid.toString()) + " size: " + groupsWithExceptions.size());
    System.out.println("###### " + group.size() + " - " + !groupsWithExceptions.contains(groupIdUuid.toString()));
    return group.size() == 3 && !groupsWithExceptions.contains(groupIdUuid.toString());
  }
}

