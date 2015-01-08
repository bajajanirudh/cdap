/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.schedule;

import co.cask.cdap.api.workflow.ScheduleProgramInfo;

import java.util.Set;

/**
 * Specification for {@link Schedule}.
 */
public final class ScheduleSpecification {
  private final Schedule schedule;
  private final Set<ScheduleProgramInfo> programs;

  public ScheduleSpecification(Schedule schedule, Set<ScheduleProgramInfo> programs) {
    this.schedule = schedule;
    this.programs = programs;
  }

  /**
   * @return the {@link Set} of programs associated with {@link ScheduleSpecification}
   */
  public Set<ScheduleProgramInfo> getPrograms() {
    return programs;
  }

  /**
   * @return the {@link Schedule} associated with {@link ScheduleSpecification}
   */
  public Schedule getSchedule() {
    return schedule;
  }
}
