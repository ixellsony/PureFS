# encoding: utf-8
require 'sinatra'
require 'sqlite3'
require 'bcrypt'
require 'json'
require 'httparty'
require 'fileutils'

# Configuration
set :sessions, true
set :session_secret, 'q5ze4d6az4ed6a4ze6f4qz5ef64qze4faz46e1fazef14azef41azef841a4zr9ef41aze41f'
set :bind, '0.0.0.0'
set :port, 4567

# Configuration du serveur de stockage
STORAGE_SERVER = 'localhost:8080'

# Base de données SQLite
def init_db
  db = SQLite3::Database.new 'data.db'
  
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  SQL
  
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS folders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      parent_id INTEGER,
      user_id INTEGER NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(parent_id) REFERENCES folders(id),
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
  SQL
  
  db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      original_name TEXT NOT NULL,
      folder_id INTEGER,
      user_id INTEGER NOT NULL,
      size INTEGER DEFAULT 0,
      uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(folder_id) REFERENCES folders(id),
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
  SQL
  
  # Créer l'utilisateur admin par défaut
  begin
    password_hash = BCrypt::Password.create('admin')
    db.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)', ['admin', password_hash])
    puts "Utilisateur admin créé avec succès"
  rescue SQLite3::ConstraintException
    puts "Utilisateur admin existe déjà"
  end
  
  db.close
end

# Helpers pour la base de données
def get_db
  db = SQLite3::Database.new 'data.db'
  db.results_as_hash = false
  db
end

def current_user
  return nil unless session[:user_id]
  db = get_db
  user = db.execute('SELECT * FROM users WHERE id = ?', [session[:user_id]]).first
  db.close
  user
end

def require_login
  unless current_user
    session[:redirect_after_login] = request.path_info
    redirect '/login'
  end
end

# Fonction helper pour vérifier si un fichier est une image 
def is_image?(filename)
  image_extensions = %w[.jpg .jpeg .png .gif .bmp .webp .svg]
  extension = File.extname(filename).downcase
  image_extensions.include?(extension)
end

# Classe pour les requêtes vers le serveur de stockage
class StorageAPI
  include HTTParty
  base_uri "http://#{STORAGE_SERVER}"
  
  def self.upload_file(file_data, filename)
    begin
      clean_filename = filename.gsub(/[^\w\.\-]/, '_')
      
      # Créer un fichier temporaire avec le bon nom
      temp_dir = Dir.mktmpdir
      temp_file_path = File.join(temp_dir, clean_filename)
      
      File.open(temp_file_path, 'wb') do |f|
        f.write(file_data)
      end
      
      response = post('/api/files/upload', 
        body: { 
          file: File.new(temp_file_path, 'rb')
        },
        timeout: 300
      )
      
      # Nettoyer le fichier temporaire
      FileUtils.rm_rf(temp_dir)
      
      puts "Upload vers: #{clean_filename} - Code: #{response.code}"
      response.success?
      
    rescue => e
      puts "Exception upload: #{e.message}"
      # S'assurer que le répertoire temporaire est nettoyé même en cas d'erreur
      FileUtils.rm_rf(temp_dir) if temp_dir && Dir.exist?(temp_dir)
      false
    end
  end
  
  def self.download_file(filename)
    begin
      puts "Tentative de téléchargement: #{filename}"
      
      response = get("/api/files/download/#{URI.encode_www_form_component(filename)}",
        timeout: 300
      )
      
      puts "Code de réponse: #{response.code}"
      
      if response.success?
        response.body.force_encoding('BINARY')
      else
        puts "Erreur téléchargement: Code #{response.code}"
        nil
      end
      
    rescue => e
      puts "Exception téléchargement: #{e.message}"
      nil
    end
  end
  
  def self.delete_file(filename)
    begin
      response = post("/api/files/delete/#{URI.encode_www_form_component(filename)}",
        timeout: 30
      )
      response.success?
      
    rescue => e
      puts "Erreur suppression: #{e.message}"
      false
    end
  end
  
  def self.server_status
    begin
      response = get('/', timeout: 10)
      response.success?
    rescue
      false
    end
  end
end

# Initialisation
init_db

# Routes d'authentification
get '/login' do
  erb :login
end

post '/login' do
  db = get_db
  user = db.execute('SELECT * FROM users WHERE username = ?', [params[:username]]).first
  db.close
  
  if user && BCrypt::Password.new(user[2]) == params[:password]
    session[:user_id] = user[0]
    redirect_url = session.delete(:redirect_after_login) || '/'
    redirect redirect_url
  else
    @error = "Nom d'utilisateur ou mot de passe incorrect"
    erb :login
  end
end

get '/logout' do
  session.clear
  redirect '/login'
end

get '/register' do
  erb :register
end

post '/register' do
  if params[:username].to_s.strip.empty? || params[:password].to_s.strip.empty?
    @error = "Tous les champs sont requis"
    erb :register
  else
    db = get_db
    begin
      password_hash = BCrypt::Password.create(params[:password])
      db.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)', 
                [params[:username].strip, password_hash])
      db.close
      redirect '/login'
    rescue SQLite3::ConstraintException
      @error = "Ce nom d'utilisateur existe déjà"
      db.close
      erb :register
    end
  end
end

# Routes principales
get '/' do
  require_login
  @current_folder_id = params[:folder_id]&.to_i
  @user = current_user
  
  db = get_db
  
  # Récupérer le chemin actuel
  @breadcrumb = []
  current_id = @current_folder_id
  while current_id
    folder = db.execute('SELECT * FROM folders WHERE id = ? AND user_id = ?', 
                       [current_id, @user[0]]).first
    break unless folder
    @breadcrumb.unshift({ id: folder[0], name: folder[1].to_s })
    current_id = folder[2]
  end
  
  # Récupérer les dossiers
  @folders = db.execute('SELECT * FROM folders WHERE parent_id IS ? AND user_id = ? ORDER BY name', 
                       [@current_folder_id, @user[0]])
  
  # Récupérer les fichiers
  @files = db.execute('SELECT * FROM files WHERE folder_id IS ? AND user_id = ? ORDER BY name', 
                     [@current_folder_id, @user[0]])
  
  db.close
  erb :index
end

post '/create_folder' do
  require_login
  user = current_user
  folder_name = params[:folder_name].to_s.strip
  parent_id = params[:parent_id].to_s.strip.empty? ? nil : params[:parent_id].to_i
  
  if folder_name.empty?
    redirect back
  end
  
  db = get_db
  db.execute('INSERT INTO folders (name, parent_id, user_id) VALUES (?, ?, ?)', 
            [folder_name, parent_id, user[0]])
  db.close
  
  redirect back
end

# Route AJAX pour upload de fichier
post '/upload_file_ajax' do
  require_login
  user = current_user
  folder_id = params[:folder_id].to_s.strip.empty? ? nil : params[:folder_id].to_i
  
  content_type :json
  
  unless params[:file] && params[:file][:tempfile]
    return { success: false, message: "Aucun fichier sélectionné" }.to_json
  end
  
  original_filename = params[:file][:filename].to_s
  clean_filename = original_filename.gsub(/[^\w\.\-]/, '_')
  
  # Lire le fichier en mode binaire
  file_data = File.binread(params[:file][:tempfile].path)
  
  # Générer un nom unique pour le stockage
  timestamp = Time.now.to_i
  storage_filename = "#{user[0]}_#{timestamp}_#{clean_filename}"
  
  puts "Upload AJAX: #{original_filename} -> #{storage_filename}"
  
  # Upload vers le système de stockage distribué
  if StorageAPI.upload_file(file_data, storage_filename)
    db = get_db
    db.execute('INSERT INTO files (name, original_name, folder_id, user_id, size) VALUES (?, ?, ?, ?, ?)', 
              [storage_filename, original_filename, folder_id, user[0], file_data.length])
    db.close
    puts "Upload AJAX réussi pour: #{original_filename}"
    { success: true, message: "Fichier uploadé avec succès", filename: original_filename }.to_json
  else
    puts "Échec upload AJAX pour: #{original_filename}"
    { success: false, message: "Erreur lors de l'upload du fichier" }.to_json
  end
end

post '/upload_file' do
  require_login
  user = current_user
  folder_id = params[:folder_id].to_s.strip.empty? ? nil : params[:folder_id].to_i
  
  unless params[:file] && params[:file][:tempfile]
    redirect back
  end
  
  original_filename = params[:file][:filename].to_s
  clean_filename = original_filename.gsub(/[^\w\.\-]/, '_')
  
  # Lire le fichier en mode binaire
  file_data = File.binread(params[:file][:tempfile].path)
  
  # Générer un nom unique pour le stockage
  timestamp = Time.now.to_i
  storage_filename = "#{user[0]}_#{timestamp}_#{clean_filename}"
  
  puts "Upload: #{original_filename} -> #{storage_filename}"
  
  # Upload vers le système de stockage distribué
  if StorageAPI.upload_file(file_data, storage_filename)
    db = get_db
    db.execute('INSERT INTO files (name, original_name, folder_id, user_id, size) VALUES (?, ?, ?, ?, ?)', 
              [storage_filename, original_filename, folder_id, user[0], file_data.length])
    db.close
    puts "Upload réussi pour: #{original_filename}"
  else
    puts "Échec upload pour: #{original_filename}"
  end
  
  redirect back
end

get '/download/:id' do
  require_login
  user = current_user
  
  db = get_db
  file = db.execute('SELECT * FROM files WHERE id = ? AND user_id = ?', 
                   [params[:id].to_i, user[0]]).first
  db.close
  
  unless file
    halt 404, "Fichier non trouvé"
  end
  
  storage_filename = file[1].to_s  # name (nom de stockage)
  original_filename = file[2].to_s # original_name
  
  file_data = StorageAPI.download_file(storage_filename)
  
  unless file_data
    halt 500, "Erreur lors du téléchargement du fichier depuis le serveur de stockage"
  end
  
  # Headers pour le téléchargement
  response.headers['Content-Type'] = 'application/octet-stream'
  response.headers['Content-Disposition'] = "attachment; filename=\"#{original_filename}\""
  response.headers['Content-Length'] = file_data.length.to_s
  
  # Retourner les données binaires
  file_data
end

post '/delete_file/:id' do
  require_login
  user = current_user
  
  db = get_db
  file = db.execute('SELECT * FROM files WHERE id = ? AND user_id = ?', 
                   [params[:id].to_i, user[0]]).first
  
  if file
    # Supprimer du stockage distribué
    StorageAPI.delete_file(file[1].to_s)
    
    # Supprimer de la base de données
    db.execute('DELETE FROM files WHERE id = ? AND user_id = ?', 
              [params[:id].to_i, user[0]])
  end
  
  db.close
  redirect back
end

post '/delete_folder/:id' do
  require_login
  user = current_user
  folder_id = params[:id].to_i
  
  db = get_db
  
  # Fonction récursive pour supprimer dossiers et fichiers
  def delete_folder_recursive(db, folder_id, user_id)
    # Supprimer tous les fichiers du dossier
    files = db.execute('SELECT * FROM files WHERE folder_id = ? AND user_id = ?', [folder_id, user_id])
    files.each do |file|
      StorageAPI.delete_file(file[1].to_s)
      db.execute('DELETE FROM files WHERE id = ?', [file[0]])
    end
    
    # Supprimer tous les sous-dossiers récursivement
    subfolders = db.execute('SELECT id FROM folders WHERE parent_id = ? AND user_id = ?', [folder_id, user_id])
    subfolders.each do |subfolder|
      delete_folder_recursive(db, subfolder[0], user_id)
    end
    
    # Supprimer le dossier lui-même
    db.execute('DELETE FROM folders WHERE id = ? AND user_id = ?', [folder_id, user_id])
  end
  
  delete_folder_recursive(db, folder_id, user[0])
  db.close
  
  redirect back
end

post '/rename_folder/:id' do
  require_login
  user = current_user
  new_name = params[:new_name].to_s.strip
  
  unless new_name.empty?
    db = get_db
    db.execute('UPDATE folders SET name = ? WHERE id = ? AND user_id = ?', 
              [new_name, params[:id].to_i, user[0]])
    db.close
  end
  
  redirect back
end

post '/rename_file/:id' do
  require_login
  user = current_user
  new_name = params[:new_name].to_s.strip
  
  unless new_name.empty?
    db = get_db
    db.execute('UPDATE files SET original_name = ? WHERE id = ? AND user_id = ?', 
              [new_name, params[:id].to_i, user[0]])
    db.close
  end
  
  redirect back
end

# Route pour récupérer la liste des fichiers en AJAX
get '/api/files' do
  require_login
  user = current_user
  folder_id = params[:folder_id]&.to_i
  
  content_type :json
  
  db = get_db
  
  # Récupérer les dossiers
  folders = db.execute('SELECT * FROM folders WHERE parent_id IS ? AND user_id = ? ORDER BY name', 
                      [folder_id, user[0]])
  
  # Récupérer les fichiers
  files = db.execute('SELECT * FROM files WHERE folder_id IS ? AND user_id = ? ORDER BY name', 
                    [folder_id, user[0]])
  
  db.close
  
  {
    folders: folders.map { |f| { id: f[0], name: f[1], created_at: f[4] } },
    files: files.map { |f| { 
      id: f[0], 
      storage_name: f[1], 
      original_name: f[2], 
      size: f[5], 
      uploaded_at: f[6] 
    } }
  }.to_json
end

# Route pour l'API (statut du serveur)
get '/api/status' do
  content_type :json
  {
    status: 'ok', 
    storage_server: StorageAPI.server_status ? 'online' : 'offline'
  }.to_json
end

# Ajoutez cette route après les autres routes de téléchargement dans app.rb

# Route pour prévisualiser une image
get '/preview/:id' do
  require_login
  user = current_user
  
  db = get_db
  file = db.execute('SELECT * FROM files WHERE id = ? AND user_id = ?', 
                   [params[:id].to_i, user[0]]).first
  db.close
  
  unless file
    halt 404, "Fichier non trouvé"
  end
  
  storage_filename = file[1].to_s  # name (nom de stockage)
  original_filename = file[2].to_s # original_name
  
  # Vérifier si c'est une image
  image_extensions = %w[.jpg .jpeg .png .gif .bmp .webp .svg]
  extension = File.extname(original_filename).downcase
  
  unless image_extensions.include?(extension)
    halt 400, "Ce fichier n'est pas une image"
  end
  
  file_data = StorageAPI.download_file(storage_filename)
  
  unless file_data
    halt 500, "Erreur lors du téléchargement du fichier depuis le serveur de stockage"
  end
  
  # Déterminer le type MIME
  content_type_map = {
    '.jpg' => 'image/jpeg',
    '.jpeg' => 'image/jpeg',
    '.png' => 'image/png',
    '.gif' => 'image/gif',
    '.bmp' => 'image/bmp',
    '.webp' => 'image/webp',
    '.svg' => 'image/svg+xml'
  }
  
  content_type content_type_map[extension] || 'image/jpeg'
  
  # Retourner les données de l'image
  file_data
end
