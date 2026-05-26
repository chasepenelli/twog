import styles from '../loading-skeleton.module.css';

export default function LeaderboardLoading() {
  return (
    <div className="site-shell page-shell" aria-busy="true" aria-live="polite">
      <div className={styles.shell}>
        <div className={styles.header}>
          <p className={styles.kicker}>TWOG / Leaderboard</p>
          <div
            className={`${styles.skeleton} ${styles.skeletonHeading}`}
            aria-hidden="true"
          />
          <div
            className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
            aria-hidden="true"
          />
        </div>
        <div className={styles.podium} aria-hidden="true">
          <div className={styles.podiumStep}>
            <span className={styles.skeleton} style={{ position: 'absolute', inset: 0 }} />
          </div>
          <div className={styles.podiumStep}>
            <span className={styles.skeleton} style={{ position: 'absolute', inset: 0 }} />
          </div>
          <div className={styles.podiumStep}>
            <span className={styles.skeleton} style={{ position: 'absolute', inset: 0 }} />
          </div>
        </div>
        <div className={styles.list} aria-hidden="true">
          {Array.from({ length: 8 }).map((_, idx) => (
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
        <span className={styles.srOnly}>Loading leaderboard.</span>
      </div>
    </div>
  );
}
