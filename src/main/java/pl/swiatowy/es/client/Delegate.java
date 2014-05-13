package pl.swiatowy.es.client;

import java.lang.annotation.RetentionPolicy;

/**
 * Helper annotation for delegate designation
 *
 * @author swiatek25
 */
@java.lang.annotation.Retention(RetentionPolicy.RUNTIME)
@java.lang.annotation.Target({java.lang.annotation.ElementType.METHOD})
public @interface Delegate {
}
