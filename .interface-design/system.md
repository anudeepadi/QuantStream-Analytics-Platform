# QuantStream Design System

## Direction

Precision & Density — finance-grade data dashboard with compact metrics, floating white cards on sage background.

## Foundation

- **Color Space:** oklch
- **Hue Family:** Sage Green (hue 142)
- **Depth:** Borders + Card Shadows (borders define structure, cards float with subtle box-shadow)
- **Font:** Plus Jakarta Sans (body), Geist Mono (code/data)

## Color Tokens

### Light Mode

| Token | Value | Purpose |
|-------|-------|---------|
| `--background` | `oklch(0.964 0.024 142)` | Soft sage page background |
| `--foreground` | `oklch(0.12 0.005 142)` | Primary text |
| `--card` | `oklch(1 0 0)` | Pure white card surfaces |
| `--primary` | `oklch(0.54 0.185 142)` | Brand green (buttons, links, active nav) |
| `--primary-foreground` | `oklch(1 0 0)` | Text on primary |
| `--secondary` | `oklch(0.946 0.016 142)` | Soft sage surface |
| `--muted` | `oklch(0.946 0.016 142)` | Subdued background |
| `--muted-foreground` | `oklch(0.50 0.010 142)` | Secondary text |
| `--accent` | `oklch(0.940 0.020 142)` | Hover backgrounds |
| `--border` | `oklch(0.926 0.012 142)` | Barely-there sage border |
| `--positive` | `oklch(0.50 0.180 142)` | Financial gain (green) |
| `--negative` | `oklch(0.52 0.220 29)` | Financial loss (red) |
| `--destructive` | `oklch(0.577 0.245 27.325)` | Error / danger |

### Chart Palette

| Token | Value | Name |
|-------|-------|------|
| `--chart-1` | `oklch(0.54 0.185 142)` | Green |
| `--chart-2` | `oklch(0.58 0.150 250)` | Blue |
| `--chart-3` | `oklch(0.65 0.180 52)` | Amber |
| `--chart-4` | `oklch(0.68 0.150 295)` | Purple |
| `--chart-5` | `oklch(0.60 0.220 22)` | Red |

### Semantic Color Usage

| Class | Frequency | Purpose |
|-------|-----------|---------|
| `text-muted-foreground` | 166x | Secondary/supporting text |
| `bg-muted` | 42x | Subdued backgrounds |
| `text-negative` | 38x | Loss values, errors |
| `text-positive` | 35x | Gain values, success |
| `text-primary` | 26x | Brand-colored text |
| `bg-primary/10` | 18x | Tinted icon containers |
| `bg-yellow-500/10` | 6x | Warning backgrounds |

## Typography

### Scale

| Size | Usage | Frequency |
|------|-------|-----------|
| `text-2xl` | Page titles | per page |
| `text-[15px]` | Card titles | 4x |
| `text-[14px]` | Section titles (settings) | 4x |
| `text-[13px]` | **Dominant body text**, labels, metric names | 79x |
| `text-[12px]` | Form labels, supporting text | 19x |
| `text-[11px]` | Secondary text, timestamps, descriptions | 43x |
| `text-[10px]` | Micro badges, pill labels, timestamps | 17x |
| `text-xs` | Button text, tab pills | misc |
| `text-sm` | Descriptions, input text | misc |

### Conventions

- **Section headers:** `text-[13px] font-semibold uppercase tracking-wider text-muted-foreground`
- **Page titles:** `text-2xl font-bold tracking-tight`
- **Card titles:** `text-[15px] font-semibold` or `text-[14px] font-semibold`
- **Body text:** `text-[13px]` with `font-medium` or `font-semibold`
- **Supporting text:** `text-[11px] text-muted-foreground`
- **Micro text:** `text-[10px] text-muted-foreground/60`

## Spacing

### Base

4px (Tailwind default unit)

### Scale

| Tailwind | Pixels | Usage | Frequency |
|----------|--------|-------|-----------|
| `1` | 4px | Tight margins (mt-1) | 27x |
| `1.5` | 6px | Icon gaps (gap-1.5) | 23x |
| `2` | 8px | Small gaps, margins (gap-2) | 39x |
| `3` | 12px | Medium gaps (px-3) | 43x |
| `4` | 16px | Standard gaps (gap-4) | 29x |
| `5` | 20px | Card padding (px-5, p-5) | 46x+18x |
| `6` | 24px | Section spacing (space-y-6) | 14x |

### Dominant Pattern

- Card internal: `px-5 pt-5 pb-5` (20px padding)
- Section gap: `space-y-6` (24px vertical rhythm)
- Grid gap: `gap-4` (16px) for card grids
- Component gap: `gap-2` or `gap-3` for inline elements

## Radius

### Base

`--radius: 0.75rem` (12px)

### Scale

| Class | Pixels | Usage | Frequency |
|-------|--------|-------|-----------|
| `rounded-full` | 50% | Dots, badges, pills, avatars | 72x |
| `rounded-xl` | 16px | Cards, buttons, inputs, icon containers | 55x |
| `rounded-lg` | 12px | Tab pills, misc | 13x |
| `rounded-md` | 10px | Shadcn component defaults | 18x |
| `rounded-2xl` | 20px | Decorative, banners | 4x |

### Convention

- **Cards:** `rounded-xl`
- **Buttons (inline):** `rounded-xl`
- **Inputs:** `rounded-xl`
- **Badges/pills:** `rounded-full`
- **Shadcn defaults:** `rounded-md`

## Depth

### Strategy: Borders + Card Shadows

- **107** border usages vs **27** shadow usages
- Borders define structure everywhere
- Only cards get shadows (floating effect)

### Card Shadow

```css
/* Light */
box-shadow:
  0 1px 2px oklch(0 0 0 / 0.04),
  0 4px 16px oklch(0 0 0 / 0.06);

/* Dark */
box-shadow:
  0 1px 3px oklch(0 0 0 / 0.15),
  0 4px 16px oklch(0 0 0 / 0.20);
```

## Component Patterns

### Button

**Shadcn Component:**
- Default: `h-9 px-4 rounded-md bg-primary text-primary-foreground`
- Sizes: xs(h-6), sm(h-8), default(h-9), lg(h-10)
- Variants: default, outline, ghost, secondary, destructive, link

**Inline Buttons (custom):**
- `rounded-xl bg-primary px-4 py-2 text-xs font-semibold text-primary-foreground hover:bg-primary/90 transition-colors`
- Tab-style: `rounded-lg px-3 py-1.5 text-xs font-semibold`

### Card

- Base: `rounded-xl border bg-card shadow-sm` (via shadcn + custom shadow)
- CardHeader: override to `pt-5 px-5`
- CardContent: override to `px-5 p-5`
- Section label: `text-[13px] font-semibold uppercase tracking-wider text-muted-foreground`

### Icon Container

- Standard: `flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10`
- Icon inside: `h-4 w-4 text-primary`
- Variants: `bg-negative/10 text-negative`, `bg-yellow-500/10 text-yellow-600`

### Status Dot

- Standard: `h-2 w-2 rounded-full` with semantic color
- Mini: `h-1.5 w-1.5 rounded-full`
- Animated: add `animate-pulse` for live indicators

### Badge / Pill

- `rounded-full px-2 py-0.5 text-[10px] font-semibold`
- Colored: `bg-{semantic}/10 text-{semantic}`

### List Row

- `flex items-center justify-between rounded-xl border border-border/60 bg-muted/30 px-4 py-3`
- Status dot left, actions right
- Title: `text-[13px] font-semibold`, subtitle: `text-[11px] text-muted-foreground`

### Tab Pill Group

- Container: `flex items-center gap-1`
- Active: `rounded-lg px-3 py-1.5 text-xs font-semibold bg-primary/15 text-primary`
- Inactive: `text-muted-foreground hover:text-foreground`

### Stats Card (KPI)

- Card with `p-5`
- Label: `text-[13px] font-semibold uppercase tracking-wider text-muted-foreground`
- Value: `text-2xl font-bold`
- Subtitle: `text-xs text-muted-foreground`
- Icon container top-right: `h-8 w-8 rounded-xl bg-primary/10`

### Symbol Selector

- Container: `flex flex-wrap items-center gap-1.5`
- Active: `rounded-xl px-3 py-1.5 text-xs font-semibold bg-primary text-primary-foreground`
- Inactive: `bg-muted text-muted-foreground hover:text-foreground`

### Progress Bar

- Track: `h-1.5 rounded-full bg-muted overflow-hidden`
- Fill: `h-full rounded-full transition-all` with color class + width style

### Live Indicator

- `flex items-center gap-1.5 text-[11px] text-positive font-semibold`
- Dot: `h-1.5 w-1.5 rounded-full bg-positive animate-pulse`

## Grid Patterns

- Stats row: `grid gap-4 sm:grid-cols-2 lg:grid-cols-4`
- Metrics grid: `grid gap-4 sm:grid-cols-2 lg:grid-cols-3`
- Main layout: `grid gap-5 lg:grid-cols-3` (2/3 + 1/3 via `lg:col-span-2`)
- Indicator grid: `grid gap-3 sm:grid-cols-2 lg:grid-cols-4`

## Loading States

- Card skeleton: `CardSkeleton` component with pulse animation
- Table skeleton: `TableSkeleton` with configurable rows
- Pattern: `{isLoading && !data ? <Skeleton /> : <Content />}`
- Prevents flash when React Query cache provides instant data
