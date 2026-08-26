# 미국 종가 대시보드 (나스닥100 + S&P500)

매일 자동으로 나스닥100 + S&P500 종목의 종가/전일대비 등락률을 수집해서
정적 웹페이지로 보여주는 프로젝트예요. GitHub Actions가 매일 데이터를 갱신하고,
GitHub Pages가 그 결과를 웹페이지로 서빙합니다.

## 처음 설정하는 방법 (한 번만 하면 돼요)

1. **GitHub에 새 저장소(Repository) 만들기**
   - github.com에서 New repository → 이름 자유롭게 (예: `us-stock-dashboard`)
   - Public으로 만들면 GitHub Actions 무료 사용량이 무제한이라 더 편해요.

2. **이 폴더 안의 파일들을 그대로 저장소에 업로드**
   - 폴더 구조 그대로 유지해야 해요 (`.github/workflows/`, `scripts/`, `data/` 등)
   - GitHub 웹사이트에서 "Add file → Upload files"로 드래그해서 올리셔도 되고,
     git에 익숙하시면 `git add . && git commit -m "init" && git push`로 올리셔도 돼요.

3. **워크플로우 쓰기 권한 켜기** (이거 빼먹으면 매일 갱신이 "조용히" 실패해요)
   - 저장소 → Settings → Actions → General → 아래로 스크롤
   - "Workflow permissions"에서 **"Read and write permissions"** 선택 → Save

4. **GitHub Pages 켜기**
   - 저장소 → Settings → Pages
   - Source: "Deploy from a branch" → Branch: `main` / `/(root)` 선택 → Save
   - 몇 분 후 `https://<사용자명>.github.io/<저장소이름>/` 주소로 접속 가능

5. **첫 데이터 수집을 수동으로 한 번 실행해보기**
   - 저장소 → Actions 탭 → "Update US Stock Prices" 워크플로우 선택
   - 오른쪽의 "Run workflow" 버튼 클릭 → 수동 실행
   - 몇 분 뒤 `data/prices.json`이 갱신된 커밋이 생기면 성공!
   - 페이지를 새로고침하면 데이터가 표에 나타나요.

이후로는 매일 한국시간 오전 8시(UTC 23:00)에 자동으로 실행돼요.
`.github/workflows/update-prices.yml`의 `cron` 값을 바꾸면 시간을 조정할 수 있어요.

## 문제가 생기면

- Actions 탭에서 최근 실행 기록을 눌러보면 어느 단계에서 실패했는지 로그로 확인할 수 있어요.
- 60일 동안 저장소에 아무 커밋도 없으면 GitHub이 스케줄을 자동으로 꺼버려요.
  (이 워크플로우는 매일 스스로 커밋을 만들기 때문에 정상 작동하는 한 이 문제는 안 생겨요.
  혹시 며칠 이상 안 돌고 있다면 Actions 탭에서 "Enable workflow"가 떠 있는지 확인해보세요.)
- 종목 리스트는 `tickers_cache.json`에 저장되고 7일마다 자동 갱신돼요. 수동으로 강제 갱신하고
  싶으면 이 파일을 지우고 워크플로우를 다시 실행하면 돼요.
