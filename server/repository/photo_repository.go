package repository

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mqtt-streaming-server/domain"
)

var ErrInvalidFilter = errors.New("invalid filter field")

type photoRepository struct {
	db *mongo.Database
}

func NewPhotoRepository(db *mongo.Database) *photoRepository {
	return &photoRepository{db: db}
}

func (repo *photoRepository) GetPhotos(ctx context.Context, filters map[string]any) ([]*domain.Photo, error) {
	allowedFields := map[string]bool{"user_id": true, "timestamp": true, "camera_id": true}
	for key := range filters {
		if !allowedFields[key] {
			return nil, fmt.Errorf("%w: %s", ErrInvalidFilter, key)
		}
	}

	collection := repo.db.Collection("photos")
	photos := make([]*domain.Photo, 0)
	cursor, err := collection.Find(ctx, filters, &options.FindOptions{
		Sort: map[string]int{"timestamp": -1},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var photo domain.Photo
		if err := cursor.Decode(&photo); err != nil {
			return nil, err
		}
		photos = append(photos, &photo)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return photos, nil
}

func (repo *photoRepository) Save(ctx context.Context, photo *domain.Photo) error {
	collection := repo.db.Collection("photos")
	_, err := collection.InsertOne(ctx, photo)
	return err
}

func (repo *photoRepository) GetByID(ctx context.Context, id string) (*domain.Photo, error) {
	collection := repo.db.Collection("photos")
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}
	var photo domain.Photo
	err = collection.FindOne(ctx, map[string]any{"_id": objID}).Decode(&photo)
	if err != nil {
		return nil, err
	}
	return &photo, nil
}

func (repo *photoRepository) Delete(ctx context.Context, id string) error {
	collection := repo.db.Collection("photos")
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = collection.DeleteOne(ctx, map[string]any{"_id": objID})
	return err
}

func (repo *photoRepository) DeleteAll(ctx context.Context) (int64, error) {
	collection := repo.db.Collection("photos")
	result, err := collection.DeleteMany(ctx, map[string]any{})
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}
