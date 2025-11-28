package ch.admin.bit.jeap.messaging.sequentialinbox.jpa;

record CountByType(String messageType, long waitingCount) {
}
