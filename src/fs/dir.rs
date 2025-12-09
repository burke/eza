// SPDX-FileCopyrightText: 2024 Christina Sørensen
// SPDX-License-Identifier: EUPL-1.2
//
// SPDX-FileCopyrightText: 2023-2024 Christina Sørensen, eza contributors
// SPDX-FileCopyrightText: 2014 Benjamin Sago
// SPDX-License-Identifier: MIT
use crate::fs::feature::git::GitCache;
use crate::fs::fields::GitStatus;
use std::fs;
use std::fs::DirEntry;
use std::io;
use std::path::{Path, PathBuf};
use std::slice::Iter as SliceIter;
use std::collections::HashSet;

use log::{info, warn, debug};
use serde::Deserialize;

use crate::fs::File;

#[derive(Deserialize)]
struct Manifest {
    #[serde(flatten)]
    pub entries: std::collections::HashMap<String, serde_json::Value>,
}

/// Cached manifest information for a src root
pub struct ManifestInfo {
    pub src_root: PathBuf,
    pub entries: HashSet<String>,
}

impl ManifestInfo {
    /// Check if a target path (relative to `src_root`) is a zone
    pub fn is_zone(&self, target_path: &str) -> bool {
        self.entries.contains(target_path)
    }

    /// Build the target path string for a file given its canonical path
    pub fn target_path_for(&self, canonical_path: &Path) -> Option<String> {
        let rel_path = canonical_path.strip_prefix(&self.src_root).ok()?;
        if rel_path.as_os_str().is_empty() {
            None
        } else {
            Some(format!("//{}", rel_path.to_string_lossy()))
        }
    }
}

/// Find manifest by walking up from the given path looking for src/.meta/manifest.json
pub fn find_manifest(start_path: &Path) -> Option<ManifestInfo> {
    let canonical_path = match start_path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            debug!("Failed to canonicalize path {start_path:?}: {e}");
            return None;
        }
    };

    // Find the src root
    let mut current = canonical_path.as_path();
    let mut src_root = None;

    loop {
        if current.file_name().is_some_and(|n| n == "src") {
            let manifest_path = current.join(".meta/manifest.json");
            if manifest_path.exists() {
                src_root = Some(current.to_path_buf());
                break;
            }
        }
        match current.parent() {
            Some(p) => current = p,
            None => break,
        }
    }

    let src_root = src_root?;

    // Read manifest
    let manifest_path = src_root.join(".meta/manifest.json");
    let file = match std::fs::File::open(&manifest_path) {
        Ok(f) => f,
        Err(e) => {
            debug!("Failed to open manifest at {manifest_path:?}: {e}");
            return None;
        }
    };
    let reader = io::BufReader::new(file);
    let manifest: Manifest = match serde_json::from_reader(reader) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to parse manifest at {manifest_path:?}: {e}");
            return None;
        }
    };

    let entries: HashSet<String> = manifest.entries.keys().cloned().collect();

    Some(ManifestInfo { src_root, entries })
}

fn get_ghosts<'dir>(dir: &'dir Dir, manifest_info: Option<&ManifestInfo>) -> Vec<File<'dir>> {
    let Some(manifest_info) = manifest_info else {
        return vec![];
    };

    let Ok(canonical_path) = dir.path.canonicalize() else {
        return vec![];
    };

    // Determine relative path and prefix
    let Ok(rel_path) = canonical_path.strip_prefix(&manifest_info.src_root) else {
        return vec![];
    };

    let prefix = if rel_path.as_os_str().is_empty() {
        "//".to_string()
    } else {
        format!("//{}/", rel_path.to_string_lossy())
    };

    // Identify ghost children (both direct and intermediate)
    let existing_names: HashSet<String> = dir.contents.iter()
        .map(|e| File::filename(&e.path()))
        .collect();

    // Track all intermediate directories we need to create ghosts for
    let mut ghost_names: HashSet<String> = HashSet::new();

    for key in &manifest_info.entries {
        if let Some(suffix) = key.strip_prefix(&prefix) {
            if !suffix.is_empty() {
                // Get the first component of the path
                let first_component = suffix.split('/').next().unwrap();

                // If this component doesn't exist physically, it should be a ghost
                if !existing_names.contains(first_component) {
                    ghost_names.insert(first_component.to_string());
                }
            }
        }
    }

    // Create ghost nodes for all identified names
    let mut ghosts = Vec::new();
    for name in ghost_names {
        let ghost_path = dir.path.join(&name);
        // Check if this ghost is itself a zone
        let ghost_target = format!("{prefix}{name}");
        let is_zone = manifest_info.is_zone(&ghost_target);
        ghosts.push(File::new_ghost(ghost_path, dir, name, is_zone));
    }

    ghosts
}

/// A **Dir** provides a cached list of the file paths in a directory that’s
/// being listed.
///
/// This object gets passed to the Files themselves, in order for them to
/// check the existence of surrounding files, then highlight themselves
/// accordingly. (See `File#get_source_files`)
pub struct Dir {
    /// A vector of the files that have been read from this directory.
    contents: Vec<DirEntry>,

    /// The path that was read.
    pub path: PathBuf,
}

impl Dir {
    /// Create a new, empty `Dir` object representing the directory at the given path.
    ///
    /// This function does not attempt to read the contents of the directory; it merely
    /// initializes an instance of `Dir` with an empty `DirEntry` list and the specified path.
    /// To populate the `Dir` object with actual directory contents, use the `read` function.
    pub fn new(path: PathBuf) -> Self {
        Self {
            contents: vec![],
            path,
        }
    }

    /// Reads the contents of the directory into `DirEntry`.
    ///
    /// It is recommended to use this method in conjunction with `new` in recursive
    /// calls, rather than `read_dir`, to avoid holding multiple open file descriptors
    /// simultaneously, which can lead to "too many open files" errors.
    pub fn read(&mut self) -> io::Result<&Self> {
        info!("Reading directory {:?}", &self.path);

        self.contents = fs::read_dir(&self.path)?.collect::<Result<Vec<_>, _>>()?;

        info!("Read directory success {:?}", &self.path);
        Ok(self)
    }

    /// Create a new Dir object filled with all the files in the directory
    /// pointed to by the given path. Fails if the directory can’t be read, or
    /// isn’t actually a directory, or if there’s an IO error that occurs at
    /// any point.
    ///
    /// The `read_dir` iterator doesn’t actually yield the `.` and `..`
    /// entries, so if the user wants to see them, we’ll have to add them
    /// ourselves after the files have been read.
    pub fn read_dir(path: PathBuf) -> io::Result<Self> {
        info!("Reading directory {:?}", &path);

        let contents = fs::read_dir(&path)?.collect::<Result<Vec<_>, _>>()?;

        info!("Read directory success {:?}", &path);
        Ok(Self { contents, path })
    }

    /// Produce an iterator of IO results of trying to read all the files in
    /// this directory.
    #[must_use]
    pub fn files<'dir, 'ig>(
        &'dir self,
        dots: DotFilter,
        git: Option<&'ig GitCache>,
        git_ignoring: bool,
        deref_links: bool,
        total_size: bool,
        no_ghosts: bool,
    ) -> Files<'dir, 'ig> {
        // Load manifest once for this directory
        let manifest_info = find_manifest(&self.path);

        let ghosts = if no_ghosts {
            vec![]
        } else {
            get_ghosts(self, manifest_info.as_ref())
        };

        Files {
            inner: self.contents.iter(),
            dir: self,
            dotfiles: dots.shows_dotfiles(),
            dots: dots.dots(),
            git,
            git_ignoring,
            deref_links,
            total_size,
            ghosts: ghosts.into_iter(),
            manifest_info,
        }
    }

    /// Whether this directory contains a file with the given path.
    #[must_use]
    pub fn contains(&self, path: &Path) -> bool {
        self.contents.iter().any(|p| p.path().as_path() == path)
    }

    /// Append a path onto the path specified by this directory.
    #[must_use]
    pub fn join(&self, child: &Path) -> PathBuf {
        self.path.join(child)
    }
}

/// Iterator over reading the contents of a directory as `File` objects.
#[allow(clippy::struct_excessive_bools)]
pub struct Files<'dir, 'ig> {
    /// The internal iterator over the paths that have been read already.
    inner: SliceIter<'dir, DirEntry>,

    /// The directory that begat those paths.
    dir: &'dir Dir,

    /// Whether to include dotfiles in the list.
    dotfiles: bool,

    /// Whether the `.` or `..` directories should be produced first, before
    /// any files have been listed.
    dots: DotsNext,

    git: Option<&'ig GitCache>,

    git_ignoring: bool,

    /// Whether symbolic links should be dereferenced when querying information.
    deref_links: bool,

    /// Whether to calculate the directory size recursively
    total_size: bool,

    /// Iterator over ghost files to be displayed
    ghosts: std::vec::IntoIter<File<'dir>>,

    /// Manifest info for determining zone status
    manifest_info: Option<ManifestInfo>,
}

impl<'dir> Files<'dir, '_> {
    fn parent(&self) -> PathBuf {
        // We can’t use `Path#parent` here because all it does is remove the
        // last path component, which is no good for us if the path is
        // relative. For example, while the parent of `/testcases/files` is
        // `/testcases`, the parent of `.` is an empty path. Adding `..` on
        // the end is the only way to get to the *actual* parent directory.
        self.dir.path.join("..")
    }

    /// Go through the directory until we encounter a file we can list (which
    /// varies depending on the dotfile visibility flag)
    fn next_visible_file(&mut self) -> Option<File<'dir>> {
        loop {
            if let Some(entry) = self.inner.next() {
                let path = entry.path();
                let filename = File::filename(&path);
                if !self.dotfiles && filename.starts_with('.') {
                    continue;
                }

                // Also hide _prefix files on Windows because it's used by old applications
                // as an alternative to dot-prefix files.
                #[cfg(windows)]
                if !self.dotfiles && filename.starts_with('_') {
                    continue;
                }

                if self.git_ignoring {
                    let git_status = self.git.map(|g| g.get(&path, false)).unwrap_or_default();
                    if git_status.unstaged == GitStatus::Ignored {
                        continue;
                    }
                }

                let mut file = File::from_args(
                    path.clone(),
                    self.dir,
                    filename,
                    self.deref_links,
                    self.total_size,
                    entry.file_type().ok(),
                );

                // Check if this file is a zone (only for directories)
                if file.is_directory() {
                    if let Some(ref manifest) = self.manifest_info {
                        // Need to canonicalize the path for comparison with manifest entries
                        if let Ok(canonical) = path.canonicalize() {
                            if let Some(target_path) = manifest.target_path_for(&canonical) {
                                file.is_zone = manifest.is_zone(&target_path);
                            }
                        }
                    }
                }

                // Windows has its own concept of hidden files, when dotfiles are
                // hidden Windows hidden files should also be filtered out
                #[cfg(windows)]
                if !self.dotfiles && file.attributes().map_or(false, |a| a.hidden) {
                    continue;
                }

                return Some(file);
            }

            return None;
        }
    }
}

/// The dot directories that need to be listed before actual files, if any.
/// If these aren’t being printed, then `FilesNext` is used to skip them.
enum DotsNext {
    /// List the `.` directory next.
    Dot,

    /// List the `..` directory next.
    DotDot,

    /// Forget about the dot directories and just list files.
    Files,
}

impl<'dir> Iterator for Files<'dir, '_> {
    type Item = File<'dir>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.dots {
            DotsNext::Dot => {
                self.dots = DotsNext::DotDot;
                Some(File::new_aa_current(self.dir, self.total_size))
            }

            DotsNext::DotDot => {
                self.dots = DotsNext::Files;
                Some(File::new_aa_parent(
                    self.parent(),
                    self.dir,
                    self.total_size,
                ))
            }

            DotsNext::Files => {
                if let Some(f) = self.next_visible_file() {
                    return Some(f);
                }
                self.ghosts.next()
            }
        }
    }
}

/// Usually files in Unix use a leading dot to be hidden or visible, but two
/// entries in particular are “extra-hidden”: `.` and `..`, which only become
/// visible after an extra `-a` option.
#[derive(PartialEq, Eq, Debug, Default, Copy, Clone)]
pub enum DotFilter {
    /// Shows files, dotfiles, and `.` and `..`.
    DotfilesAndDots,

    /// Show files and dotfiles, but hide `.` and `..`.
    Dotfiles,

    /// Just show files, hiding anything beginning with a dot.
    #[default]
    JustFiles,
}

impl DotFilter {
    /// Whether this filter should show dotfiles in a listing.
    fn shows_dotfiles(self) -> bool {
        match self {
            Self::JustFiles => false,
            Self::Dotfiles => true,
            Self::DotfilesAndDots => true,
        }
    }

    /// Whether this filter should add dot directories to a listing.
    fn dots(self) -> DotsNext {
        match self {
            Self::JustFiles => DotsNext::Files,
            Self::Dotfiles => DotsNext::Files,
            Self::DotfilesAndDots => DotsNext::Dot,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod manifest_info {
        use super::*;

        fn make_manifest(entries: &[&str]) -> ManifestInfo {
            ManifestInfo {
                src_root: PathBuf::from("/test/src"),
                entries: entries.iter().map(|s| s.to_string()).collect(),
            }
        }

        #[test]
        fn is_zone_returns_true_for_matching_entry() {
            let manifest = make_manifest(&["//areas/tools/dev", "//areas/apps/flow"]);
            assert!(manifest.is_zone("//areas/tools/dev"));
            assert!(manifest.is_zone("//areas/apps/flow"));
        }

        #[test]
        fn is_zone_returns_false_for_non_matching_entry() {
            let manifest = make_manifest(&["//areas/tools/dev"]);
            assert!(!manifest.is_zone("//areas/tools"));
            assert!(!manifest.is_zone("//areas/tools/dev/subdir"));
            assert!(!manifest.is_zone("//other/path"));
        }

        #[test]
        fn target_path_for_builds_correct_path() {
            let manifest = make_manifest(&[]);

            let path = Path::new("/test/src/areas/tools/dev");
            assert_eq!(
                manifest.target_path_for(path),
                Some("//areas/tools/dev".to_string())
            );
        }

        #[test]
        fn target_path_for_returns_none_for_src_root() {
            let manifest = make_manifest(&[]);

            let path = Path::new("/test/src");
            assert_eq!(manifest.target_path_for(path), None);
        }

        #[test]
        fn target_path_for_returns_none_for_path_outside_src() {
            let manifest = make_manifest(&[]);

            let path = Path::new("/other/path");
            assert_eq!(manifest.target_path_for(path), None);
        }
    }
}
