package io.realm.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

class Realm implements Plugin<Project> {

    @Override
    void apply(Project project) {
    	project.repositories.add(project.repositories.jcenter())
        project.dependencies.add('compile', 'io.realm:realm-android:0.83.0') // TODO: make version dynamic
    }
}
