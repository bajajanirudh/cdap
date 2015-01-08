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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.schedule.SchedulableProgramType;

/**
 * Represents name and type information of the program.
 */
public final class ScheduleProgramInfo {
  private final String programName;
  private final SchedulableProgramType programType;

  public ScheduleProgramInfo(String programName, SchedulableProgramType programType) {
    this.programName = programName;
    this.programType = programType;
  }

  /**
   *
   * @return name of the {@link SchedulableProgramType}
   */
  public String getProgramName() {
    return programName;
  }

  /**
   *
   * @return type of the {@link SchedulableProgramType}
   */
  public SchedulableProgramType getProgramType() {
    return programType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ScheduleProgramInfo that = (ScheduleProgramInfo) o;

    if (!programName.equals(that.programName)) {
      return false;
    }
    if (programType != that.programType) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = programName.hashCode();
    result = 31 * result + programType.hashCode();
    return result;
  }
}
