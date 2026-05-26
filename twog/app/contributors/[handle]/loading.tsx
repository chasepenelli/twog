import styles from '../../loading-skeleton.module.css';

export default function ContributorHandleLoading() {
  return (
    <div className="site-shell page-shell" aria-busy="true" aria-live="polite">
      <div className={styles.shell}>
        <div className={styles.header}>
          <p className={styles.kicker}>TWOG / Contributor</p>
          <div
            className={`${styles.skeleton} ${styles.skeletonHeading}`}
            aria-hidden="true"
          />
          <div
            className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
            aria-hidden="true"
          />
          <div
            className={`${styles.skeleton} ${styles.skeletonRow} ${styles.short}`}
            aria-hidden="true"
          />
        </div>
        <div className={styles.hero} aria-hidden="true">
          <div className={styles.skeletonCard}>
            <div
              className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
            />
            <div className={`${styles.skeleton} ${styles.skeletonRow}`} />
            <div
              className={`${styles.skeleton} ${styles.skeletonRow} ${styles.short}`}
            />
          </div>
        </div>
        <div className={styles.list} aria-hidden="true">
          {Array.from({ length: 5 }).map((_, idx) => (
            <div key={idx} className={styles.skeletonRowLine}>
              <div
                className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
              />
              <div
                className={`${styles.skeleton} ${styles.skeletonRow} ${styles.short}`}
              />
            </div>
          ))}
        </div>
        <span className={styles.srOnly}>Loading contributor profile.</span>
      </div>
    </div>
  );
}
