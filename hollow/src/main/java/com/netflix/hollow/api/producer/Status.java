/*
 *
 *  Copyright 2018 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.api.producer;

import java.time.Duration;

/**
 * A status representing success or failure for a particular producer action.
 */
public abstract class Status {
    final StatusType type;
    final Throwable cause;

    Status(StatusType type, Throwable cause) {
        this.type = type;
        this.cause = cause;
    }

    /**
     * The status type.
     */
    public enum StatusType {
        /**
         * If the producer action was successful.
         */
        SUCCESS,

        /**
         * If the producer action failed.
         */
        FAIL;
    }

    /**
     * Gets the status type
     *
     * @return the status type
     */
    public StatusType getType() {
        return type;
    }

    /**
     * Returns the cause of producer action failure.
     *
     * @return the cause of producer action failure
     */
    public Throwable getCause() {
        return cause;
    }

    /**
     * A status representing success or failure for a particular producer stage.
     */
    public static class Stage extends Status {
        final long version;

        /**
         * Constructs a new stage status
         *
         * @param type the status type
         * @param cause the cause
         * @param version the version
         */
        public Stage(
                StatusType type, Throwable cause,
                long version) {
            super(type, cause);
            this.version = version;
        }

        /**
         * Gets the version associated with the producer stage
         * @return the version associated with the producer stage
         */
        public long getVersion() {
            return version;
        }
    }

    /**
     * A status representing success or failure for a particular producer stage associated
     * with {@link HollowProducer.ReadState read state}.
     */
    public static class StageWithState extends Stage {
        final HollowProducer.ReadState readState;

        /**
         * Constructs a new stage with state status
         *
         * @param type the status type
         * @param cause the cause
         * @param version the version
         * @param readState the read state
         */
        public StageWithState(
                StatusType type, Throwable cause,
                long version, HollowProducer.ReadState readState) {
            super(type, cause, version);
            this.readState = readState;
        }

        /**
         * Gets the read state associated with the producer stage
         * @return the read state associated with the producer stage
         */
        public HollowProducer.ReadState getReadState() {
            return readState;
        }
    }

    /**
     * A status representing success or failure for a publish action.
     */
    public static class Publish extends Status {
        private final HollowProducer.Blob blob;

        /**
         * Constructs a new publish status.
         *
         * @param type the status type
         * @param cause the cause
         * @param blob the blob that was published
         */
        public Publish(
                StatusType type, Throwable cause,
                HollowProducer.Blob blob) {
            super(type, cause);
            this.blob = blob;
        }

        /**
         * Gets the blob that was published.
         *
         * @return the blob that was published
         */
        public HollowProducer.Blob getBlob() {
            return blob;
        }
    }

    /**
     * A status representing success or failure for a restore stage.
     */
    public static class RestoreStage extends Status {
        private final long versionDesired;
        private final long versionReached;

        /**
         * Constructs a new restore stage status.
         *
         * @param type the status type
         * @param cause the cause
         * @param versionDesired the desired version to restore to
         * @param versionReached the actual version restored to
         */
        public RestoreStage(
                StatusType type, Throwable cause,
                long versionDesired, long versionReached) {
            super(type, cause);
            this.versionDesired = versionDesired;
            this.versionReached = versionReached;
        }

        /**
         * Gets the desired version to restore to.
         *
         * @return the desired version to restore to
         */
        public long getVersionDesired() {
            return versionDesired;
        }

        /**
         * Gets the actual version restored to.
         *
         * @return the actual version restored to
         */
        public long getVersionReached() {
            return versionReached;
        }
    }

    abstract static class AbstractStatusBuilder {
        long start;
        long end;

        AbstractStatusBuilder() {
            start = System.currentTimeMillis();
        }

        void finish() {
            end = System.currentTimeMillis();
        }

        Duration elapsed() {
            return Duration.ofMillis(end - start);
        }
    }

    static class StageBuilder extends AbstractStatusBuilder {
        StatusType type;
        Throwable cause;
        long version;

        StageBuilder version(long version) {
            this.version = version;
            return this;
        }

        StageBuilder success() {
            this.type = StatusType.SUCCESS;
            return this;
        }

        StageBuilder fail(Throwable cause) {
            this.type = StatusType.FAIL;
            this.cause = cause;
            return this;
        }

        Stage build() {
            finish();
            return new Stage(type, cause, version);
        }
    }

    static class StageWithStateBuilder extends AbstractStatusBuilder {
        StatusType type;
        Throwable cause;
        long version;
        HollowProducer.ReadState readState;

        StageWithStateBuilder version(long version) {
            this.version = version;
            return this;
        }

        StageWithStateBuilder readState(HollowProducer.ReadState readState) {
            this.readState = readState;
            return version(readState.getVersion());
        }

        StageWithStateBuilder success() {
            this.type = StatusType.SUCCESS;
            return this;
        }

        StageWithStateBuilder fail(Throwable cause) {
            this.type = StatusType.FAIL;
            this.cause = cause;
            return this;
        }

        StageWithState build() {
            finish();
            return new StageWithState(type, cause, version, readState);
        }
    }

    static class PublishBuilder extends AbstractStatusBuilder {
        StatusType type;
        Throwable cause;
        HollowProducer.Blob blob;

        PublishBuilder blob(HollowProducer.Blob blob) {
            this.blob = blob;
            return this;
        }

        PublishBuilder success() {
            this.type = StatusType.SUCCESS;
            return this;
        }

        PublishBuilder fail(Throwable cause) {
            this.type = StatusType.FAIL;
            this.cause = cause;
            return this;
        }

        Publish build() {
            finish();
            return new Publish(type, cause, blob);
        }
    }

    static class RestoreStageBuilder extends AbstractStatusBuilder {
        StatusType type;
        Throwable cause;
        long versionDesired;
        long versionReached;

        RestoreStageBuilder versions(long versionDesired, long versionReached) {
            this.versionDesired = versionDesired;
            this.versionReached = versionReached;
            return this;
        }

        RestoreStageBuilder success() {
            this.type = StatusType.SUCCESS;
            return this;
        }

        RestoreStageBuilder fail(Throwable cause) {
            this.type = StatusType.FAIL;
            this.cause = cause;
            return this;
        }

        RestoreStage build() {
            finish();
            return new RestoreStage(type, cause, versionDesired, versionReached);
        }
    }
}
